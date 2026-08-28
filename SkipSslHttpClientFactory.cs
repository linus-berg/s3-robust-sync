using System.Net.Http;
using Amazon.Runtime;

namespace S3RobustSync;

/// <summary>
/// Custom HttpClientFactory that disables SSL certificate validation.
/// Used when connecting to MinIO instances with self-signed certificates.
/// </summary>
public class SkipSslHttpClientFactory : HttpClientFactory
{
    public override HttpClient CreateHttpClient(IClientConfig clientConfig)
    {
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true
        };

        return new HttpClient(handler);
    }
}
