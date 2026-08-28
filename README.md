# S3 Robust Sync

S3 Robust Sync is a highly resilient, high-performance command-line utility written in .NET to seamlessly synchronize massive datasets (millions of files, large objects) from a local MinIO bucket to a remote AWS S3 bucket.

## Key Features

- **Infinite Resilience:** Designed to handle severe network interruptions (even hours of downtime). Uses Polly for exponential backoff retries. When the internet comes back, the sync seamlessly resumes exactly where it left off.
- **Stateful Resumption:** Utilizes a local SQLite database (`sync_state.db`) with Write-Ahead Logging (WAL) to persistently track successfully transferred objects. If the program is restarted, it will instantly skip over already synced files.
- **Continuation Token Caching:** Safely checkpoints your MinIO pagination (`ContinuationToken`) directly into the database. If your sync of 70 million files crashes halfway through, it resumes *instantly* at the exact cursor position without rescanning the entire bucket!
- **High-Performance Parallelism:** Transfers multiple files concurrently and uses AWS `TransferUtility` to automatically chunk large 5GB+ files into memory-efficient multipart uploads.
- **One-Shot Execution:** Operates as a definitive script—it aggressively syncs the entire bucket in a single pass and exits cleanly upon completion, making it perfect for CI/CD pipelines or cron jobs.
- **One-Way Sync:** Designed purely as a pusher mechanism. It only lists files in the local MinIO bucket and streams them to AWS S3, dramatically reducing AWS API costs since it never lists or checks the remote AWS bucket directly.

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

Alternatively, cross-platform single-file executables are automatically compiled and attached to GitHub Releases via GitHub Actions!

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
  --ignore-token            Ignore saved continuation token and restart scan from the beginning (Default: false)
  -h, --help                Show help message
```

### Examples

**Basic Sync:**
```bash
dotnet run -- --minio-bucket "my-local-bucket" --aws-bucket "my-remote-bucket"
```

**High-Speed Sync with Prefix & Forced Rescan:**
```bash
dotnet run -- \
  --minio-bucket "sensor-data" \
  --aws-bucket "production-sensor-data" \
  --prefix "2026/08/" \
  --parallelism 16 \
  --ignore-token
```

## How It Works

1. **Initialization**: Creates a local SQLite database `sync_state.db` in the working directory to track state. It configures connection-pooling and WAL mode to handle massively concurrent I/O safely.
2. **Scan**: Paginates through all files in the specified MinIO bucket (optionally filtering by `--prefix`). It automatically picks up from the cached pagination token if a previous run was aborted.
3. **Filter**: Instantly skips any files that are already present in the SQLite database.
4. **Transfer**: Streams the files directly from MinIO into AWS S3 using concurrent threads (dictated by `--parallelism`). 
5. **Mark & Save**: Once the file is successfully uploaded to AWS, the object key is recorded in the SQLite database, and the MinIO cursor (`ContinuationToken`) is checkpointed.
6. **Completion**: Once the entire bucket is scanned and uploaded, the process exits automatically.

If any transfer or listing operation fails (e.g. lost internet connection), the built-in Polly retry policy catches the exception, logs an error, and waits before trying again on *that specific file or API call*. It will continue attempting indefinitely until it succeeds, completely eliminating the need for manual babysitting.
