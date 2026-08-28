# S3 Robust Sync

S3 Robust Sync is a highly resilient command-line utility written in .NET to seamlessly synchronize objects from a local MinIO bucket to a remote AWS S3 bucket.

## Key Features

- **Infinite Resilience:** Designed to handle severe network interruptions (even hours of downtime). Uses Polly for exponential backoff retries. When the internet comes back, the sync seamlessly resumes exactly where it left off.
- **Stateful Resumption:** Utilizes a local SQLite database (`sync_state.db`) to persistently track successfully transferred objects. If the program is restarted, it will not redundantly copy files that have already been pushed.
- **One-Way Sync:** Designed purely as a pusher mechanism. It only lists files in the local MinIO bucket and streams them to AWS S3, dramatically reducing AWS API costs since it never lists or checks the remote AWS bucket directly.
- **Prefix Filtering:** Selectively sync specific directories or file patterns using a source prefix.

## Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download) (or compatible version)
- Access to a MinIO instance
- AWS Account with S3 PutObject permissions

## Installation

You can build the source code directly:

```bash
git clone <repository-url>
cd s3-robust-sync
dotnet build
```

To create a standalone executable that you can run without the `dotnet` prefix:

```bash
# Example for macOS ARM64
dotnet publish -c Release -r osx-arm64 --self-contained true -p:PublishSingleFile=true
```

## Usage

S3 Robust Sync is built using the Cocona CLI framework. You can run it directly via `dotnet run` (make sure to use `--` before your arguments):

```bash
dotnet run -- [options]
```

### Options

```text
  --minio-url <String>      Local MinIO URL (Default: http://localhost:9000)
  --minio-access <String>   MinIO Access Key (Default: minioadmin)
  --minio-secret <String>   MinIO Secret Key (Default: minioadmin)
  --minio-bucket <String>   MinIO Bucket (Default: local-bucket)
  --aws-region <String>     AWS Region (Default: us-east-1)
  --aws-access <String>     AWS Access Key (Default: aws_access_key)
  --aws-secret <String>     AWS Secret Key (Default: aws_secret_key)
  --aws-bucket <String>     AWS Bucket (Default: aws-bucket)
  --prefix <String>         Prefix to filter objects in MinIO
  -p, --parallelism <Int32> Number of concurrent uploads (Default: 4)
  -h, --help                Show help message
```

### Examples

**Basic Sync:**
```bash
dotnet run -- --minio-bucket "my-local-bucket" --aws-bucket "my-remote-bucket"
```

**Sync with Custom Credentials and Prefix:**
```bash
dotnet run -- \
  --minio-url "http://192.168.1.50:9000" \
  --minio-access "local_key" \
  --minio-secret "local_secret" \
  --minio-bucket "sensor-data" \
  --aws-region "us-west-2" \
  --aws-access "AKIA..." \
  --aws-secret "secret..." \
  --aws-bucket "production-sensor-data" \
  --prefix "2026/08/"
```

## How It Works

1. **Initialization**: Creates a local SQLite database `sync_state.db` in the working directory to track state.
2. **Scan**: Paginates through all files in the specified MinIO bucket (optionally filtering by `--prefix`).
3. **Filter**: Skips any files that are already present in the SQLite database.
4. **Transfer**: Streams the file directly from MinIO into AWS S3. 
5. **Mark**: Once the file is successfully uploaded to AWS, the object key is recorded in the SQLite database.
6. **Continuous Polling**: After thoroughly scanning and transferring all objects, the script idles for 30 seconds before repeating the process to look for new files.

If any transfer or listing operation fails (e.g. lost internet connection), the built-in Polly retry policy catches the exception, logs an error, and waits before trying again. The wait time increases exponentially up to a maximum of 60 seconds per retry, and it will continue attempting indefinitely until it succeeds.
