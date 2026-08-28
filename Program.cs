using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Cocona;

namespace S3RobustSync;

class Program
{
    static async Task Main(string[] args)
    {
        var app = CoconaApp.Create();

        app.AddCommand(async (
            [Option("minio-url", Description = "Local MinIO URL")] string minioUrl = "http://localhost:9000",
            [Option("minio-access", Description = "MinIO Access Key")] string minioAccessKey = "minioadmin",
            [Option("minio-secret", Description = "MinIO Secret Key")] string minioSecretKey = "minioadmin",
            [Option("minio-bucket", Description = "MinIO Bucket")] string minioBucket = "local-bucket",
            [Option("aws-region", Description = "AWS Region")] string awsRegion = "us-east-1",
            [Option("aws-access", Description = "AWS Access Key")] string awsAccessKey = "aws_access_key",
            [Option("aws-secret", Description = "AWS Secret Key")] string awsSecretKey = "aws_secret_key",
            [Option("aws-bucket", Description = "AWS Bucket")] string awsBucket = "aws-bucket",
            [Option("prefix", Description = "Prefix to filter objects in MinIO")] string? prefix = null
        ) =>
        {
            Console.WriteLine("Starting S3 Robust Sync...");
            if (!string.IsNullOrEmpty(prefix))
            {
                Console.WriteLine($"Filtering by prefix: {prefix}");
            }

            string dbPath = "sync_state.db";

            using var repository = new SyncStateRepository(dbPath);

            var minioConfig = new AmazonS3Config
            {
                ServiceURL = minioUrl,
                ForcePathStyle = true,
            };
            using var minioClient = new AmazonS3Client(minioAccessKey, minioSecretKey, minioConfig);

            var awsConfig = new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(awsRegion)
            };
            using var awsClient = new AmazonS3Client(awsAccessKey, awsSecretKey, awsConfig);

            var syncService = new S3SyncService(
                minioClient,
                awsClient,
                repository,
                minioBucket,
                awsBucket,
                prefix);

            await syncService.RunContinuousSyncAsync();
        });

        await app.RunAsync();
    }
}
