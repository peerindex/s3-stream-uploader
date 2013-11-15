# s3-stream-uploader

## About

A utility class for stream uploading large amounts of data into Amazon S3 using [Multipart Upload][1]

[1]:http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html

## Usage

```java
AmazonS3Client s3Client = // Configure your Amazon S3 client

// How long to wait for part uploads before failing the upload
long uploadTimeoutSec = 300;

// The data is stored as multiple files on S3 with this size (in this case 1GB)
int fileSizeBytes = 1024 * 1024 * 1024;

S3StreamFactory factory = new S3StreamFactory(cl, TimeUnit.SECONDS, 300, fileSizeBytes, false);

String bucketName = "testbucket";

// The data is stored as multiple files (file names are generated using UUID) under the specified prefix
String filePrefix = "test/";

S3Stream stream = factory.newStream(bucketName, filePrefix, Charsets.UTF_8);
try{
    for (int i = 0; i < 20000; i++) {
        stream.write("content");
    }
} finally {
    stream.close();
}

```

"permitFailures" parameter:  
If permitFailures=true is specified, upload attempts that failed are logged and ignored. This is useful when you can't retry the upload anyways. If set to false, the stream is closed on whenever an upload failure happens and you have to open a new stream to write data again.

Other important Notes:  
Both factory and streams are thread-safe. However, note that there is no guarantee on the order each data is appended to the file. You typically pass a line to `write`.
The factory is meant to be used as a singleton, as it uses a thread pool that is shared across streams created by  the same factory.

Even if permitFailure=false is specified, data that was already uploaded to S3 will not be deleted upon failure. Therefore, same data may get stored more than once if you retry the upload after a failure.


## License

Apache 2.0

## Quickstart

1. Get the code: `git clone git://github.com/peerindex/s3-stream-uploader.git`
1. Install the jar `mvn install`
1. Explore the javadoc `mvn javadoc:javadoc`

## Dependency
This library depends on the following libraries:
```
       <!-- Metrics -->
        <dependency>
            <groupId>com.codahale.metrics</groupId>
            <artifactId>metrics-core</artifactId>
            <version>3.0.1</version>
            <scope>provided</scope>
        </dependency>

        <!-- Amazon -->
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>aws-java-sdk</artifactId>
            <version>1.4.3</version>
            <scope>provided</scope>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.6.6</version>
            <scope>provided</scope>
        </dependency>

        <!-- Utilities -->
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>14.0.1</version>
            <scope>provided</scope>
        </dependency>
```

