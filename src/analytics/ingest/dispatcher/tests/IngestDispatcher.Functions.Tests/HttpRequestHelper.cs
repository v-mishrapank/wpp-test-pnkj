using System.Text;
using Microsoft.AspNetCore.Http;

namespace IngestDispatcher.Functions.Tests;

internal static class HttpRequestHelper
{
    public static HttpRequest BuildJsonRequest(string jsonBody, params (string Key, string Value)[] headers)
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Method = "POST";
        ctx.Request.ContentType = "application/json";
        ctx.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(jsonBody));
        foreach (var (k, v) in headers) ctx.Request.Headers[k] = v;
        return ctx.Request;
    }
}
