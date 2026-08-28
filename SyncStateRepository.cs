using System;
using Microsoft.Data.Sqlite;

namespace S3RobustSync;

public class SyncStateRepository : IDisposable
{
    private readonly SqliteConnection _connection;

    public SyncStateRepository(string dbPath)
    {
        _connection = new SqliteConnection($"Data Source={dbPath}");
        _connection.Open();
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        var command = _connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS SyncedFiles (
                ObjectKey TEXT PRIMARY KEY
            );";
        command.ExecuteNonQuery();
    }

    public bool IsFileSynced(string key)
    {
        var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM SyncedFiles WHERE ObjectKey = $key";
        command.Parameters.AddWithValue("$key", key);

        using var reader = command.ExecuteReader();
        return reader.HasRows;
    }

    public void MarkFileSynced(string key)
    {
        var command = _connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO SyncedFiles (ObjectKey) VALUES ($key)";
        command.Parameters.AddWithValue("$key", key);
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}
