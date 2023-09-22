using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentFTP;
using MoveAkatsukiReplays;

DotEnv.Load(Path.Combine(Directory.GetCurrentDirectory(), ".env"));

var ftpHost = Environment.GetEnvironmentVariable("FTP_HOST")!;
var ftpPort = int.Parse(Environment.GetEnvironmentVariable("FTP_PORT")!);
var ftpUsername = Environment.GetEnvironmentVariable("FTP_USER")!;
var ftpPassword = Environment.GetEnvironmentVariable("FTP_PASS")!;
var awsAccessKey  = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID")!;
var awsSecretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY")!;
var awsBucketName = Environment.GetEnvironmentVariable("AWS_BUCKET_NAME")!;
var awsEndpointUrl = Environment.GetEnvironmentVariable("AWS_ENDPOINT_URL")!;

using var ftp = new FtpClient();
ftp.Host = ftpHost;
ftp.Port = ftpPort;
ftp.Credentials = new NetworkCredential(ftpUsername, ftpPassword);
ftp.Connect();

using var s3 = new AmazonS3Client(
    new BasicAWSCredentials(awsAccessKey, awsSecretKey),
    new AmazonS3Config
    {
        ServiceURL = awsEndpointUrl
    });

await Parallel.ForEachAsync(ftp.GetListing("/replays"), async (x, cancellationToken) =>
{
    var getRequest = new GetObjectRequest
    {
        BucketName = awsBucketName,
        Key = $"replays/{x.Name}"
    };

    try
    {
        var getResponse = await s3.GetObjectAsync(getRequest, cancellationToken);

        if (getResponse.HttpStatusCode == HttpStatusCode.OK)
        {
            // already exists on s3, fuck off
            return;
        }
    }
    catch (AmazonS3Exception)
    {
        // doesn't exist, ignore
    }

    if (!x.Name.EndsWith(".osr"))
    {
        // not a replay file, skip
        return;
    }

    using var fileStream = new MemoryStream();
    ftp.DownloadStream(fileStream, x.Name);
    
    var putRequest = new PutObjectRequest
    {
        BucketName = awsBucketName,
        Key = $"replays/{x.Name}",
        InputStream = fileStream,
    };

    try
    {
        var putResponse = await s3.PutObjectAsync(putRequest, cancellationToken);
        if (putResponse?.HttpStatusCode != HttpStatusCode.OK)
        {
            throw new Exception($"Failed to save replay {x.Name}, status code: {putResponse?.HttpStatusCode}");
        }
        
        Console.WriteLine($"Saved replay {x.Name}");
    }
    catch (Exception)
    {
        Console.WriteLine($"Failed to save replay: {x.Name}");
        throw;
    }
});