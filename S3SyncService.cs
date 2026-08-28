using System;
using System.Threading;
using System.Threading.Tasks;
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
    private readonly AsyncRetryPolicy _retryPolicy;

    public S3SyncService(
        IAmazonS3 minioClient,
        IAmazonS3 awsClient,
        SyncStateRepository repository,
        string minioBucket,
        string awsBucket,
        string? prefix,
        int parallelism,
        bool ignoreToken)
    {
        _minioClient = minioClient;
        _awsClient = awsClient;
        _repository = repository;
        _minioBucket = minioBucket;
        _awsBucket = awsBucket;
        _prefix = prefix;
        _parallelism = parallelism;
        _ignoreToken = ignoreToken;

        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryForeverAsync(
                sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, retryAttempt))),
                onRetry: (exception, timespan) =>
                {
                    Console.WriteLine($"[ERROR] Operation failed. Retrying in {timespan.TotalSeconds} seconds... Exception: {exception.Message}");
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
                    if (_repository.IsFileSynced(s3Object.Key))
                    {
                        return; // skip if already synced
                    }

                    Console.WriteLine($"[{DateTime.UtcNow:O}] Syncing: {s3Object.Key} ({(s3Object.Size / 1024.0 / 1024.0):F2} MB)");

                    // 2. Isolate the transfer with its own retry.
                    await _retryPolicy.ExecuteAsync(async () =>
                    {
                        var getRequest = new GetObjectRequest
                        {
                            BucketName = _minioBucket,
                            Key = s3Object.Key
                        };

                        using var getResponse = await _minioClient.GetObjectAsync(getRequest, cancellationToken);

                        // 3. Use TransferUtility to handle files > 5GB via automatic multipart upload chunking
                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            InputStream = getResponse.ResponseStream,
                            Key = s3Object.Key,
                            BucketName = _awsBucket,
                            ContentType = getResponse.Headers.ContentType
                        };

                        await transferUtility.UploadAsync(uploadRequest, cancellationToken);
                    });

                    // 4. Record as synced ONLY after complete success
                    _repository.MarkFileSynced(s3Object.Key);
                    Console.WriteLine($"[{DateTime.UtcNow:O}] Successfully synced: {s3Object.Key}");
                });

                continuationToken = listResponse.NextContinuationToken;
                
                // Safely store the token so if we crash right now, we resume at this exact page
                _repository.SaveContinuationToken(continuationToken);

            } while (continuationToken != null);

            Console.WriteLine($"[{DateTime.UtcNow:O}] Sync completely finished. Exiting.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CRITICAL] Unexpected error: {ex.Message}");
            throw;
        }
    }
}
