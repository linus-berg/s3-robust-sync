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
            [Option("prefix", Description = "Prefix to filter objects in MinIO")] string? prefix = null,
            [Option('p', Description = "Number of concurrent uploads")] int parallelism = 4,
            [Option("db-path", Description = "Path to the SQLite database file")] string dbPath = "sync_state.db",
            [Option("temp-dir", Description = "Directory for temporary files during large file transfers")] string? tempDir = null,
            [Option("skip-ssl", Description = "Skip SSL certificate validation for the MinIO connection")] bool skipSsl = false,
            [Option("log-file", Description = "Path to a log file (output is written to both console and file)")] string? logFile = null
        ) =>
        {
            // Setup log tee if requested
            using var teeWriter = new TeeTextWriter(Console.Out, logFile);
            if (!string.IsNullOrEmpty(logFile))
            {
                Console.SetOut(teeWriter);
            }

            // Startup config summary
            Console.WriteLine("╔══════════════════════════════════════════════════╗");
            Console.WriteLine("║             S3 Robust Sync                      ║");
            Console.WriteLine("╠══════════════════════════════════════════════════╣");
            Console.WriteLine($"║  MinIO URL:       {minioUrl}");
            Console.WriteLine($"║  MinIO Bucket:    {minioBucket}");
            Console.WriteLine($"║  AWS Region:      {awsRegion}");
            Console.WriteLine($"║  AWS Bucket:      {awsBucket}");
            Console.WriteLine($"║  Prefix:          {prefix ?? "(none)"}");
            Console.WriteLine($"║  Parallelism:     {parallelism}");
            Console.WriteLine($"║  DB Path:         {dbPath}");
            Console.WriteLine($"║  Temp Dir:        {tempDir ?? "(system default)"}");
            Console.WriteLine($"║  Skip SSL:        {skipSsl}");
            Console.WriteLine($"║  Log File:        {logFile ?? "(none)"}");
            Console.WriteLine("╚══════════════════════════════════════════════════╝");
            Console.WriteLine();

            var repository = new SyncStateRepository(dbPath);

            // Allow enough HTTP connections for all parallel workers + headroom for listing
            int maxConnections = parallelism + 4;

            var minioConfig = new AmazonS3Config
            {
                ServiceURL = minioUrl,
                ForcePathStyle = true,
                HttpClientFactory = new ConfigurableHttpClientFactory(maxConnections, skipSsl),
                Timeout = TimeSpan.FromMinutes(15),
            };

            if (skipSsl)
            {
                Console.WriteLine("WARNING: SSL certificate validation is disabled for MinIO.");
            }

            using var minioClient = new AmazonS3Client(minioAccessKey, minioSecretKey, minioConfig);

            var awsConfig = new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(awsRegion),
                HttpClientFactory = new ConfigurableHttpClientFactory(maxConnections),
                Timeout = TimeSpan.FromMinutes(15),
            };
            using var awsClient = new AmazonS3Client(awsAccessKey, awsSecretKey, awsConfig);

            if (!string.IsNullOrEmpty(tempDir))
            {
                Directory.CreateDirectory(tempDir);
            }

            var syncService = new S3SyncService(
                minioClient,
                awsClient,
                repository,
                minioBucket,
                awsBucket,
                prefix,
                parallelism,
                tempDir);

            await syncService.RunSyncAsync();
        });

        await app.RunAsync();
    }
}
