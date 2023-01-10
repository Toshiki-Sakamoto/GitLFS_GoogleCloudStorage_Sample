using Google.Cloud.Functions.Framework;
using Microsoft.AspNetCore.Http;
using Google.Cloud.Storage.V1;
using System.Threading.Tasks;
using System;
using System.Net.Http;
using System.IO;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http.Extensions;
using System.Xml.Linq;
using System.Linq;

namespace GDriveLFS
{
    /// <summary>
    /// 署名付きURL
    /// </summary>
    public class V4SignedUrlGenerator
    {
        public string GenerateV4SignedReadUrl(
            string bucketName,
            string objectName,
            string credentialFilePath)
        {
            UrlSigner urlSigner = UrlSigner.FromServiceAccountPath(credentialFilePath);
            string url = urlSigner.Sign(bucketName, objectName, TimeSpan.FromHours(1), HttpMethod.Get);

            return url;
        }

        public string GenerateV4UploadSignedUrl(
            string bucketName,
            string objectName,
            string credentialFilePath)
        {
            UrlSigner urlSigner = UrlSigner.FromServiceAccountPath(credentialFilePath);

            var options = UrlSigner.Options.FromDuration(TimeSpan.FromHours(1));

            var template = UrlSigner.RequestTemplate
                .FromBucket(bucketName)
                .WithObjectName(objectName)
                .WithHttpMethod(HttpMethod.Put);

            string url = urlSigner.Sign(template, options);
            return url;
        }
    }


    public class GCSStorageController
    {
        public const string BacketName = "[GCSバケット名]";
        public const string CredentialFileName = "[サービスアカウントアクセスキー.json]"// credential file (with private keys)

        private V4SignedUrlGenerator _urlGenerator = new V4SignedUrlGenerator();
        private string _credentialFilePath;

        public GCSStorageController()
        {
            _credentialFilePath = $"{Directory.GetCurrentDirectory()}/{CredentialFileName}";
        }

        public string GetDownloadURL(string objectName)
        {
            if (string.IsNullOrEmpty(_credentialFilePath)) return string.Empty;

            var result = _urlGenerator.GenerateV4SignedReadUrl(BacketName, objectName, _credentialFilePath);
            return result;
        }

        public string GetUploadURL(string objectName)
        {
            if (string.IsNullOrEmpty(_credentialFilePath)) return string.Empty;

            var result = _urlGenerator.GenerateV4UploadSignedUrl(BacketName, objectName, _credentialFilePath);
            return result;
        }
    }


    public class RequestBody
    {
        public class Object
        {
            public string oid { get; set; }
            public int size { get; set; }
        }

        public string operation { get; set; }
        public string[] transfers { get; set; }
        public Object[] objects { get; set; }
        public string hash_algo { get; set; }
    }


    public class ResponseBody
    {
        public class URL
        {
            public string href { get; set; }
            public int expires_in { get; set; }

            public static URL CreateAtDownload(string oid) =>
                new URL { href = Function.StorageController.GetDownloadURL(oid), expires_in = 86400 };

            public static URL CreateAtUpload(string oid) =>
                new URL { href = Function.StorageController.GetUploadURL(oid), expires_in = 86400 };

            public static URL CreateAtEmpty() =>
                new URL { href = "" };
        }

        public class Action
        {
            public URL upload { get; set; }
            public URL download { get; set; }

            public static Action CreateAtUpload(string oid) =>
                new Action() { upload = URL.CreateAtUpload(oid), download = URL.CreateAtEmpty() };

            public static Action CreateAtDownload(string oid) =>
                new Action() { download = URL.CreateAtDownload(oid), upload = URL.CreateAtEmpty() };
        }

        public class Object
        {
            public string oid { get; set; }
            public int size { get; set; }
            public bool authenticated { get; set; } = true;
            public Action actions { get; set; }

            public static Object CreateAtUpload(string oid, int size) =>
                new Object { oid = oid, size = size, actions = Action.CreateAtUpload(oid) };

            public static Object CreateAtDownload(string oid, int size) =>
                new Object { oid = oid, size = size, actions = Action.CreateAtDownload(oid) };
        }

        public string transfer { get; set; } = "basic";
        public Object[] objects { get; set; }


        public static ResponseBody CreateAtUpload(RequestBody.Object[] objects) =>
            new ResponseBody { objects = objects.Select(x => Object.CreateAtUpload(x.oid, x.size)).ToArray() };

        public static ResponseBody CreateAtDownload(RequestBody.Object[] objects) =>
            new ResponseBody { objects = objects.Select(x => Object.CreateAtDownload(x.oid, x.size)).ToArray() };
    }

    public class Function : IHttpFunction
    {
        public static GCSStorageController StorageController;
        private readonly ILogger _logger;

        public Function(ILogger<Function> logger) =>
            _logger = logger;

        public async Task HandleAsync(HttpContext context)
        {
            var request = context.Request;

            // If there's a body, parse it as JSON and check for "message" field.
            using TextReader reader = new StreamReader(request.Body);
            string text = await reader.ReadToEndAsync();

            if (text.Length <= 0)
            {
                _logger.LogError("RequestBody Not found");
                return;
            }

            _logger.LogInformation($"*** Request: {text}");

            try
            {
                var requestBody = JsonSerializer.Deserialize<RequestBody>(text);

                StorageController = new GCSStorageController();

                var response = default(ResponseBody);

                switch (requestBody.operation)
                {
                    case "upload":
                        response = ResponseBody.CreateAtUpload(requestBody.objects);
                        break;

                    case "download":
                        response = ResponseBody.CreateAtDownload(requestBody.objects);
                        break;

                    default: // verify
                        return;
                }

                var responseBody = JsonSerializer.Serialize<ResponseBody>(response);

                _logger.LogInformation($"*** Response: {responseBody}");

                context.Response.ContentType = "application/vnd.git-lfs+json";
                await context.Response.WriteAsJsonAsync<ResponseBody>(response);
            }
            catch (JsonException parseException)
            {
                _logger.LogError(parseException, "Error parsing JSON request");
            }
        }
    }
}
