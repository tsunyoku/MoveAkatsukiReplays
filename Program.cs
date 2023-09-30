using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentFTP;
using MoveAkatsukiReplays;

Console.WriteLine($"hi: {Path.Combine(Directory.GetCurrentDirectory(), ".env")}");

DotEnv.Load(Path.Combine(Directory.GetCurrentDirectory(), ".env"));

var ftpHost = Environment.GetEnvironmentVariable("FTP_HOST")!;
var ftpPort = int.Parse(Environment.GetEnvironmentVariable("FTP_PORT")!);
var ftpUsername = Environment.GetEnvironmentVariable("FTP_USER")!;
var ftpPassword = Environment.GetEnvironmentVariable("FTP_PASS")!;
var awsAccessKey  = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID")!;
var awsSecretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY")!;
var awsBucketName = Environment.GetEnvironmentVariable("AWS_BUCKET_NAME")!;
var awsEndpointUrl = Environment.GetEnvironmentVariable("AWS_ENDPOINT_URL")!;

var ftp = new AsyncFtpClient(ftpHost, ftpUsername, ftpPassword, ftpPort);
await ftp.AutoConnect();

var s3 = new AmazonS3Client(
    new BasicAWSCredentials(awsAccessKey, awsSecretKey),
    new AmazonS3Config
    {
        ServiceURL = awsEndpointUrl
    });

var replays = await ftp.GetListing("/replays");
Console.WriteLine($"Got {replays.Length} replays from FTP");

await Parallel.ForEachAsync(replays, async (x, cancellationToken) =>
{
    if (x.Type != FtpObjectType.File)
    {
        // not a file, skip
        return;
    }
    
    
    if (!x.Name.EndsWith(".osr"))
    {
        // not a replay file, skip
        return;
    }

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

    using var fileStream = new MemoryStream();
    await ftp.DownloadStream(fileStream, x.Name);
    
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
        await ftp.Disconnect(cancellationToken);
        throw;
    }
});

await ftp.Disconnect();