using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Dapper;
using FluentFTP;
using MoveAkatsukiReplays;
using MySqlConnector;

DotEnv.Load(Path.Combine(Directory.GetCurrentDirectory(), ".env"));

var ftpHost = Environment.GetEnvironmentVariable("FTP_HOST")!;
var ftpPort = int.Parse(Environment.GetEnvironmentVariable("FTP_PORT")!);
var ftpUsername = Environment.GetEnvironmentVariable("FTP_USER")!;
var ftpPassword = Environment.GetEnvironmentVariable("FTP_PASS")!;
var awsAccessKey  = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID")!;
var awsSecretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY")!;
var awsBucketName = Environment.GetEnvironmentVariable("AWS_BUCKET_NAME")!;
var awsEndpointUrl = Environment.GetEnvironmentVariable("AWS_ENDPOINT_URL")!;
var databaseConnectionString = Environment.GetEnvironmentVariable("DATABASE_CONNECTION_STRING")!;

var db = new MySqlConnection(databaseConnectionString);

var ftpConfig = new FtpConfig
{
    ReadTimeout = int.MaxValue,
    ConnectTimeout = int.MaxValue,
    DataConnectionConnectTimeout = int.MaxValue,
    DataConnectionReadTimeout = int.MaxValue
};
var ftp = new AsyncFtpClient(ftpHost, ftpUsername, ftpPassword, ftpPort, ftpConfig);

var s3 = new AmazonS3Client(
    new BasicAWSCredentials(awsAccessKey, awsSecretKey),
    new AmazonS3Config
    {
        ServiceURL = awsEndpointUrl
    });

async Task Run()
{
    IEnumerable<string> replays = new List<string>();
    foreach (var table in new[] { "scores", "scores_relax", "scores_ap" })
    {
        var scores = await db.QueryAsync<int>($"SELECT id FROM {table}");
        replays = replays.Union(scores.Select(x => $"replays/replay_{x}.osr"));
    }

    await Parallel.ForEachAsync(replays, async (x, cancellationToken) =>
    {
        var getRequest = new GetObjectRequest
        {
            BucketName = awsBucketName,
            Key = x,
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
        await ftp.DownloadStream(fileStream, x);
        
        var putRequest = new PutObjectRequest
        {
            BucketName = awsBucketName,
            Key = x,
            InputStream = fileStream,
        };

        try
        {
            var putResponse = await s3.PutObjectAsync(putRequest, cancellationToken);
            if (putResponse?.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception($"Failed to save replay x, status code: {putResponse?.HttpStatusCode}");
            }
            
            Console.WriteLine($"Saved replay {x}");
        }
        catch (Exception)
        {
            Console.WriteLine($"Failed to save replay: {x}");
            throw;
        }
    });
}

async Task Cleanup()
{
    await ftp.Disconnect();
    await db.CloseAsync();
}

try
{
    Console.CancelKeyPress += async (sender, eventArgs) =>
    {
        await Cleanup();
    };

    await ftp.AutoConnect();
    await db.OpenAsync();
    await Run();
}
catch (Exception ex)
{
    Console.WriteLine(ex);
}
finally
{
    await Cleanup();
}