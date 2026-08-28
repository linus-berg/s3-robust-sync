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
            );";
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
