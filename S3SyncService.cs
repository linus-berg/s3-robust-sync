using System;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
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
    private readonly AsyncRetryPolicy _retryPolicy;

    public S3SyncService(
        IAmazonS3 minioClient,
        IAmazonS3 awsClient,
        SyncStateRepository repository,
        string minioBucket,
        string awsBucket,
        string? prefix)
    {
        _minioClient = minioClient;
        _awsClient = awsClient;
        _repository = repository;
        _minioBucket = minioBucket;
        _awsBucket = awsBucket;
        _prefix = prefix;

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

    public async Task RunContinuousSyncAsync()
    {
        Console.WriteLine("Starting continuous sync loop...");

        while (true)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    string? continuationToken = null;
                    do
                    {
                        var listRequest = new ListObjectsV2Request
                        {
                            BucketName = _minioBucket,
                            ContinuationToken = continuationToken,
                            Prefix = _prefix
                        };

                        var listResponse = await _minioClient.ListObjectsV2Async(listRequest);

                        foreach (var s3Object in listResponse.S3Objects)
                        {
                            if (_repository.IsFileSynced(s3Object.Key))
                            {
                                continue;
                            }

                            Console.WriteLine($"[{DateTime.UtcNow:O}] Syncing: {s3Object.Key}");

                            var getRequest = new GetObjectRequest
                            {
                                BucketName = _minioBucket,
                                Key = s3Object.Key
                            };

                            using var getResponse = await _minioClient.GetObjectAsync(getRequest);

                            var putRequest = new PutObjectRequest
                            {
                                BucketName = _awsBucket,
                                Key = s3Object.Key,
                                InputStream = getResponse.ResponseStream,
                                ContentType = getResponse.Headers.ContentType,
                                Headers = { ContentLength = getResponse.ContentLength }
                            };

                            await _awsClient.PutObjectAsync(putRequest);

                            _repository.MarkFileSynced(s3Object.Key);
                            Console.WriteLine($"[{DateTime.UtcNow:O}] Successfully synced: {s3Object.Key}");
                        }

                        continuationToken = listResponse.NextContinuationToken;

                    } while (continuationToken != null);
                });

                await Task.Delay(TimeSpan.FromSeconds(30));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CRITICAL] Unexpected error in outer loop: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(30));
            }
        }
    }
}
