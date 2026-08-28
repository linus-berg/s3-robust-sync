using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Polly;
using Polly.Retry;

namespace S3RobustSync;

public class S3SyncService
{
    private readonly IAmazonS3 _minioClient;
    private readonly IAmazonS3 _awsClient;
    private readonly SyncStateRepository _repository;
    private readonly string _minioBucket;
    private readonly string _awsBucket;
    private readonly string? _prefix;
    private readonly int _parallelism;
    private readonly bool _ignoreToken;
    private readonly string? _tempDir;
    private readonly AsyncRetryPolicy _retryPolicy;

    /// <summary>
    /// Threshold in bytes above which files are downloaded to a temporary file
    /// before uploading (to allow TransferUtility to seek for multipart uploads).
    /// Files at or below this size are streamed directly.
    /// </summary>
    private const long LargeFileThreshold = 100 * 1024 * 1024; // 100 MB

    private long _totalProcessed;
    private long _totalSynced;
    private long _totalSkipped;

    public S3SyncService(
        IAmazonS3 minioClient,
        IAmazonS3 awsClient,
        SyncStateRepository repository,
        string minioBucket,
        string awsBucket,
        string? prefix,
        int parallelism,
        bool ignoreToken,
        string? tempDir)
    {
        _minioClient = minioClient;
        _awsClient = awsClient;
        _repository = repository;
        _minioBucket = minioBucket;
        _awsBucket = awsBucket;
        _prefix = prefix;
        _tempDir = tempDir;
        _parallelism = parallelism;
        _ignoreToken = ignoreToken;

        _retryPolicy = Policy
            .Handle<Exception>(ex => ex is not OperationCanceledException)
            .WaitAndRetryForeverAsync(
                sleepDurationProvider: retryAttempt =>
                    TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, retryAttempt)) + Random.Shared.NextDouble() * 5),
                onRetry: (exception, timespan) =>
                {
                    Console.WriteLine($"[ERROR] Operation failed. Retrying in {timespan.TotalSeconds:F1} seconds... Exception: {exception.Message}");
                }
            );
    }

    public async Task RunSyncAsync()
    {
        Console.WriteLine($"Starting one-shot sync with {_parallelism} concurrent uploads...");
        using var transferUtility = new TransferUtility(_awsClient);

        string? continuationToken = null;
        if (!_ignoreToken)
        {
            continuationToken = _repository.GetContinuationToken();
            if (!string.IsNullOrEmpty(continuationToken))
            {
                Console.WriteLine("Resuming from saved continuation token in the database...");
            }
        }

        try
        {
            do
            {
                // 1. Isolate the list operation with its own retry,
                // so if something fails, we don't lose the continuation token.
                ListObjectsV2Response listResponse = await _retryPolicy.ExecuteAsync(async () =>
                {
                    var listRequest = new ListObjectsV2Request
                    {
                        BucketName = _minioBucket,
                        ContinuationToken = continuationToken,
                        Prefix = _prefix
                    };

                    return await _minioClient.ListObjectsV2Async(listRequest);
                });

                var parallelOptions = new ParallelOptions
                {
                    MaxDegreeOfParallelism = _parallelism
                };

                await Parallel.ForEachAsync(listResponse.S3Objects, parallelOptions, async (s3Object, cancellationToken) =>
                {
                    long processed = Interlocked.Increment(ref _totalProcessed);

                    if (_repository.IsFileSynced(s3Object.Key))
                    {
                        Interlocked.Increment(ref _totalSkipped);
                        if (processed % 10_000 == 0)
                        {
                            PrintProgress();
                        }
                        return;
                    }

                    Console.WriteLine($"[{DateTime.UtcNow:O}] Syncing: {s3Object.Key} ({FormatSize(s3Object.Size.GetValueOrDefault())})");

                    // 2. Isolate the transfer with its own retry.
                    await _retryPolicy.ExecuteAsync(async () =>
                    {
                        if (s3Object.Size.GetValueOrDefault() > LargeFileThreshold)
                        {
                            // Large file path: download to temp file first so TransferUtility
                            // can seek for multipart uploads and retry individual parts.
                            await TransferViaTemporaryFile(s3Object, transferUtility, cancellationToken);
                        }
                        else
                        {
                            // Small file path: stream directly from MinIO to AWS.
                            await TransferViaStream(s3Object, transferUtility, cancellationToken);
                        }
                    });

                    // 3. Record as synced ONLY after complete success
                    _repository.MarkFileSynced(s3Object.Key);
                    long synced = Interlocked.Increment(ref _totalSynced);
                    Console.WriteLine($"[{DateTime.UtcNow:O}] Successfully synced: {s3Object.Key}");

                    if (synced % 1_000 == 0)
                    {
                        PrintProgress();
                    }
                });

                continuationToken = listResponse.NextContinuationToken;

                // Safely store the token so if we crash right now, we resume at this exact page
                _repository.SaveContinuationToken(continuationToken);

            } while (continuationToken != null);

            PrintProgress();
            Console.WriteLine($"[{DateTime.UtcNow:O}] Sync completely finished. Exiting.");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Sync cancelled.");
            throw;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CRITICAL] Unexpected error: {ex.Message}");
            throw;
        }
    }

    private async Task TransferViaStream(S3Object s3Object, TransferUtility transferUtility, CancellationToken cancellationToken)
    {
        var getRequest = new GetObjectRequest
        {
            BucketName = _minioBucket,
            Key = s3Object.Key
        };

        using var getResponse = await _minioClient.GetObjectAsync(getRequest, cancellationToken);

        var uploadRequest = new TransferUtilityUploadRequest
        {
            InputStream = getResponse.ResponseStream,
            Key = s3Object.Key,
            BucketName = _awsBucket,
            ContentType = getResponse.Headers.ContentType
        };

        await transferUtility.UploadAsync(uploadRequest, cancellationToken);
    }

    private async Task TransferViaTemporaryFile(S3Object s3Object, TransferUtility transferUtility, CancellationToken cancellationToken)
    {
        var tempFile = _tempDir != null
            ? Path.Combine(_tempDir, Path.GetRandomFileName())
            : Path.GetTempFileName();
        try
        {
            var getRequest = new GetObjectRequest
            {
                BucketName = _minioBucket,
                Key = s3Object.Key
            };

            using (var getResponse = await _minioClient.GetObjectAsync(getRequest, cancellationToken))
            await using (var fileStream = File.Create(tempFile))
            {
                await getResponse.ResponseStream.CopyToAsync(fileStream, cancellationToken);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                FilePath = tempFile,
                Key = s3Object.Key,
                BucketName = _awsBucket,
                ContentType = "application/octet-stream"
            };

            await transferUtility.UploadAsync(uploadRequest, cancellationToken);
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    private void PrintProgress()
    {
        long processed = Interlocked.Read(ref _totalProcessed);
        long synced = Interlocked.Read(ref _totalSynced);
        long skipped = Interlocked.Read(ref _totalSkipped);
        Console.WriteLine($"[{DateTime.UtcNow:O}] Progress: {processed:N0} files processed, {synced:N0} synced, {skipped:N0} skipped");
    }

    private static string FormatSize(long bytes)
    {
        return bytes switch
        {
            >= 1_073_741_824 => $"{bytes / 1_073_741_824.0:F2} GB",
            >= 1_048_576 => $"{bytes / 1_048_576.0:F2} MB",
            >= 1_024 => $"{bytes / 1_024.0:F2} KB",
            _ => $"{bytes} B"
        };
    }
}
