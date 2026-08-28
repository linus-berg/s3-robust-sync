using System;
using Microsoft.Data.Sqlite;

namespace S3RobustSync;

public class SyncStateRepository
{
    private readonly string _connectionString;

    public SyncStateRepository(string dbPath)
    {
        _connectionString = $"Data Source={dbPath}";
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = @"
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS SyncedFiles (
                ObjectKey TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS SyncMetadata (
                Key TEXT PRIMARY KEY,
                Value TEXT
            );";
        command.ExecuteNonQuery();
    }

    public string? GetContinuationToken()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = "SELECT Value FROM SyncMetadata WHERE Key = 'ContinuationToken'";
        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            return reader.GetString(0);
        }
        return null;
    }

    public void SaveContinuationToken(string? token)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        if (string.IsNullOrEmpty(token))
        {
            command.CommandText = "DELETE FROM SyncMetadata WHERE Key = 'ContinuationToken'";
        }
        else
        {
            command.CommandText = "INSERT OR REPLACE INTO SyncMetadata (Key, Value) VALUES ('ContinuationToken', $token)";
            command.Parameters.AddWithValue("$token", token);
        }
        command.ExecuteNonQuery();
    }

    public bool IsFileSynced(string key)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM SyncedFiles WHERE ObjectKey = $key";
        command.Parameters.AddWithValue("$key", key);

        using var reader = command.ExecuteReader();
        return reader.HasRows;
    }

    public void MarkFileSynced(string key)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO SyncedFiles (ObjectKey) VALUES ($key)";
        command.Parameters.AddWithValue("$key", key);
        command.ExecuteNonQuery();
    }
}
