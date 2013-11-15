package com.peerindex.s3stream;

import com.amazonaws.services.s3.AmazonS3Client;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Factory for creating {@link com.peerindex.s3stream.S3Stream} instances. This class is meant to be instantiated once,
 * and be shared across the application. This class is thread-safe.
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class S3StreamFactory {
    private static final Logger log = LoggerFactory.getLogger(S3StreamFactory.class);
    private static final AtomicInteger SEQ = new AtomicInteger();
    private static final int availableCores = Runtime.getRuntime().availableProcessors();


    private final MetricRegistry metricRegistry;
    private final AmazonS3Client s3Client;
    private final long partUploadTimeoutNs;
    private final int fileSizeBytes;
    private final int partUploadSizeBytes;
    private final boolean permitFailures;
    private final boolean enableChecksum;
    private final ListeningExecutorService workers;

    /**
     *
     * @param s3Client A properly configured {@link com.amazonaws.services.s3.AmazonS3Client}
     * @param timeUnit {@link java.util.concurrent.TimeUnit} for partUploadTimeoutDuration
     * @param partUploadTimeoutDuration The timeout duration for part uploads.
     * @param fileSizeBytes The size of files that should be created in S3. Minimum 5MB
     * @param partUploadSizeBytes The size of part uploads. Minimum 5MB
     * @param permitFailures Whether to permit failures while uploading.
     */
    public S3StreamFactory(AmazonS3Client s3Client, TimeUnit timeUnit, long partUploadTimeoutDuration, int fileSizeBytes, int partUploadSizeBytes, boolean permitFailures) {
        this(new MetricRegistry(), s3Client, timeUnit, partUploadTimeoutDuration, fileSizeBytes, partUploadSizeBytes, permitFailures, true, availableCores, availableCores * 2);
    }

    /**
     *
     * @param metricRegistry A {@link com.codahale.metrics.MetricRegistry} to output metrics to.
     * @param s3Client A properly configured {@link com.amazonaws.services.s3.AmazonS3Client}
     * @param timeUnit {@link java.util.concurrent.TimeUnit} for partUploadTimeoutDuration
     * @param partUploadTimeoutDuration The timeout duration for part uploads.
     * @param fileSizeBytes The size of files that should be created in S3. Minimum 5MB
     * @param partUploadSizeBytes The size of part uploads. Minimum 5MB
     * @param permitFailures Whether to permit failures while uploading.
     */
    public S3StreamFactory(MetricRegistry metricRegistry, AmazonS3Client s3Client, TimeUnit timeUnit, long partUploadTimeoutDuration, int fileSizeBytes, int partUploadSizeBytes, boolean permitFailures) {
        this(metricRegistry, s3Client, timeUnit, partUploadTimeoutDuration, fileSizeBytes, partUploadSizeBytes, permitFailures, true, availableCores, availableCores * 2);
    }

    /**
     *
     * @param metricRegistry A {@link com.codahale.metrics.MetricRegistry} to output metrics to.
     * @param s3Client A properly configured {@link com.amazonaws.services.s3.AmazonS3Client}
     * @param timeUnit {@link java.util.concurrent.TimeUnit} for partUploadTimeoutDuration
     * @param partUploadTimeoutDuration The timeout duration for part uploads.
     * @param fileSizeBytes The size of files that should be created in S3. Minimum 5MB
     * @param partUploadSizeBytes The size of part uploads. Minimum 5MB
     * @param permitFailures Whether to permit failures while uploading.
     * @param enableCheckSum Whether to use S3's MD5 checksum feature
     * @param workerThreadNum Number of worker threads to use for the upload
     * @param workerQueueSize Maximum size of part upload queue
     */
    public S3StreamFactory(MetricRegistry metricRegistry, AmazonS3Client s3Client, TimeUnit timeUnit, long partUploadTimeoutDuration, int fileSizeBytes, int partUploadSizeBytes, boolean permitFailures, boolean enableCheckSum,  int workerThreadNum, int workerQueueSize) {
        checkArgument(fileSizeBytes >= 5*1024*1024, "File size must be at minimum 5MB");
        checkArgument(partUploadSizeBytes >= 5*1024*1024, "Part upload size must be at minimum 5MB");
        boolean maxPartUnder10K = Math.ceil(((double)fileSizeBytes) / partUploadSizeBytes) < 10000;
        checkArgument(maxPartUnder10K, "Part upload size must be large enough so that there will be at most 10,000 part uploads to create a file. Consider increasing part upload size, or decreasing file size.");

        this.metricRegistry = metricRegistry;
        this.s3Client = s3Client;
        this.partUploadTimeoutNs = TimeUnit.NANOSECONDS.convert(partUploadTimeoutDuration,timeUnit);
        this.fileSizeBytes = fileSizeBytes;
        this.partUploadSizeBytes = partUploadSizeBytes;
        this.permitFailures = permitFailures;
        this.enableChecksum = enableCheckSum;

        BlockingQueue<Runnable> workerQueue = new LinkedBlockingQueue<Runnable>(workerQueueSize);

        ThreadFactory tf = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(S3Stream.class.getSimpleName()+"-worker-" + SEQ.incrementAndGet());
                return t;
            }
        };

        workers = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(workerThreadNum, workerThreadNum, 10, TimeUnit.HOURS, workerQueue, tf, new ThreadPoolExecutor.CallerRunsPolicy()));

    }


    public S3Stream newStream(String bucketName, String prefix, Charset charset) throws IOException {
        checkArgument(prefix.endsWith("/") && !prefix.startsWith("/"), "Prefix must not start with, and must end with \"/\". Was given:"+prefix);
        return new S3Stream(metricRegistry,s3Client, bucketName, prefix, charset, partUploadTimeoutNs, fileSizeBytes, partUploadSizeBytes, workers, permitFailures, enableChecksum);
    }
}
