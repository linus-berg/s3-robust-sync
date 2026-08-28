using System.Net.Http;
using System.Net.Security;
using Amazon.Runtime;

namespace S3RobustSync;

/// <summary>
/// Custom HttpClientFactory that configures connection pool limits
/// and optionally disables SSL certificate validation.
/// </summary>
public class ConfigurableHttpClientFactory : HttpClientFactory
{
    private readonly int _maxConnections;
    private readonly bool _skipSsl;

    public ConfigurableHttpClientFactory(int maxConnections, bool skipSsl = false)
    {
        _maxConnections = maxConnections;
        _skipSsl = skipSsl;
    }

    public override HttpClient CreateHttpClient(IClientConfig clientConfig)
    {
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = _maxConnections,
            PooledConnectionLifetime = TimeSpan.FromMinutes(15),
        };

        if (_skipSsl)
        {
            handler.SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, _, _, _) => true
            };
        }

        return new HttpClient(handler);
    }
}
