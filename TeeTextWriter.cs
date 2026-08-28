namespace S3RobustSync;

/// <summary>
/// A TextWriter that writes to both the console and an optional log file simultaneously.
/// </summary>
public class TeeTextWriter : TextWriter, IDisposable
{
    private readonly TextWriter _consoleWriter;
    private readonly StreamWriter? _fileWriter;

    public override System.Text.Encoding Encoding => _consoleWriter.Encoding;

    public TeeTextWriter(TextWriter consoleWriter, string? logFilePath)
    {
        _consoleWriter = consoleWriter;

        if (!string.IsNullOrEmpty(logFilePath))
        {
            var directory = Path.GetDirectoryName(logFilePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _fileWriter = new StreamWriter(logFilePath, append: true)
            {
                AutoFlush = true
            };
        }
    }

    public override void Write(char value)
    {
        _consoleWriter.Write(value);
        _fileWriter?.Write(value);
    }

    public override void Write(string? value)
    {
        _consoleWriter.Write(value);
        _fileWriter?.Write(value);
    }

    public override void WriteLine(string? value)
    {
        _consoleWriter.WriteLine(value);
        _fileWriter?.WriteLine(value);
    }

    public override void WriteLine()
    {
        _consoleWriter.WriteLine();
        _fileWriter?.WriteLine();
    }

    public override void Flush()
    {
        _consoleWriter.Flush();
        _fileWriter?.Flush();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _fileWriter?.Dispose();
        }
        base.Dispose(disposing);
    }
}
