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

// 1st July 2022 (we switched to s3 roughly end of june)
const int s3SwitchConstant = 1656630000;

// fetch 1000 scores at a time
const int fetchConstant = 1000;

async Task Run()
{
    var offset = 0;

    foreach (var table in new[] { "scores", "scores_relax", "scores_ap" })
    {
        while (true)
        {
            var scoreIds = (await db.QueryAsync<int>($"SELECT id FROM {table} WHERE {table}.time < {s3SwitchConstant} ORDER BY {table}.time DESC LIMIT {fetchConstant} OFFSET {offset}")).ToArray();
            offset += scoreIds.Length;
            
            foreach (var scoreId in scoreIds)
            {
                var ftpFileName = $"replays/replay_{scoreId}.osr";
                var s3FileName = $"replays/{scoreId}.osr";
                var getRequest = new GetObjectRequest
                {
                    BucketName = awsBucketName,
                    Key = s3FileName,
                };
        
                try
                {
                    var getResponse = await s3.GetObjectAsync(getRequest);
        
                    if (getResponse.HttpStatusCode == HttpStatusCode.OK)
                    {
                        Console.WriteLine($"Replay {s3FileName} already exists on S3");
                        continue;
                    }
                }
                catch (AmazonS3Exception)
                {
                    // doesn't exist, ignore
                }

                MemoryStream fileStream;
                try
                {
                    fileStream = new MemoryStream();
                    await ftp.DownloadStream(fileStream, ftpFileName);
                }
                catch (Exception)
                {
                    Console.WriteLine($"Replay {ftpFileName} doesn't exist in FTP");
                    continue;
                }
                
                var putRequest = new PutObjectRequest
                {
                    BucketName = awsBucketName,
                    Key = s3FileName,
                    InputStream = fileStream,
                };
        
                try
                {
                    var putResponse = await s3.PutObjectAsync(putRequest);
                    if (putResponse?.HttpStatusCode != HttpStatusCode.OK)
                    {
                        throw new Exception($"Failed to save replay {s3FileName}, status code: {putResponse?.HttpStatusCode}");
                    }
                    
                    Console.WriteLine($"Saved replay {s3FileName}");
                }
                catch (Exception)
                {
                    Console.WriteLine($"Failed to save replay: {s3FileName}");
                }
                
                fileStream.Dispose();
            }
    
            if (scoreIds.Length < fetchConstant)
            {
                Console.WriteLine("Done");
                break;
            }
        }
    }
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