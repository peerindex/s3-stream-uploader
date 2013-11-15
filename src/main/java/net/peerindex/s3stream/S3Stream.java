package net.peerindex.s3stream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * Utility class that can be used to stream data into Amazon S3.
 * This class is thread-safe.
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class S3Stream implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(S3Stream.class);

    // Services
    private final MetricRegistry metricRegistry;
    private final ListeningExecutorService workers;
    private final AmazonS3Client s3Client;

    // Immutable Params
    private final String bucketName;
    private final String prefix;
    private final int fileSizeBytes;
    private final int partUploadSizeBytes;
    private final long partUploadTimeoutNs;
    private final boolean permitFailures;
    private final boolean enableCheckSum;
    private final Charset charset;

    // State
    private final AtomicReference<MultipartUploadState> uploadState;
    private final Queue<ListenableFuture<Void>> pendingUploads = new ConcurrentLinkedQueue<ListenableFuture<Void>>();

    // Buffer state
    private final Object bufferLock = new Object();
    private ByteArrayOutputStream buffer;
    private boolean closed;


    S3Stream(MetricRegistry metricRegistry, AmazonS3Client s3Client, String bucketName, String prefix, Charset charset, long partUploadTimeoutNs, int fileSizeBytes, int partUploadSizeBytes, final ListeningExecutorService workers, boolean permitFailures, boolean enableChecksum) throws IOException {

        this.metricRegistry = metricRegistry;
        this.s3Client = s3Client;
        this.workers = workers;


        this.bucketName = bucketName;
        this.prefix = prefix;
        this.charset = charset;

        this.enableCheckSum = enableChecksum;
        this.fileSizeBytes = fileSizeBytes;
        this.partUploadSizeBytes = partUploadSizeBytes;
        this.partUploadTimeoutNs = partUploadTimeoutNs;
        this.permitFailures = permitFailures;

        this.uploadState = new AtomicReference<MultipartUploadState>(openNewFile(bucketName, prefix, newFileName()));
        this.buffer = new ByteArrayOutputStream(partUploadSizeBytes);
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getPrefix() {
        return prefix;
    }


    /**
     * Add new content to the buffer for upload. This method does not block and returns immediately, except when the
     * buffer and the upload queue is both full, in which case the calling thread will block until the current buffer's
     * upload finishes. This method can be called safely from multiple methods. However, note that there is no
     * guarantee on the order the content is appended to the file.
     *
     *
     * @param content
     * @throws java.io.IOException
     */
    public void write(String content) throws IOException {
        Timer.Context t = metricRegistry.timer(this.getClass().getSimpleName() + "_write").time();

        clearFinishedUploads();

        byte[] bytes = content.getBytes(charset);
        metricRegistry.meter(this.getClass().getSimpleName()+"_write_bytes").mark(bytes.length);

        synchronized (bufferLock) {
            if (closed) {
                throw new IOException("This stream is already closed");
            }
            if (buffer.size() + content.length() > partUploadSizeBytes) {
                // Buffer is full, need to flush
                schedulePartUpload(buffer, false);
                buffer = new ByteArrayOutputStream(partUploadSizeBytes);
            }
            buffer.write(bytes);
        }

        t.stop();
    }


    /**
     * This method must be called after all content has been added to the stream.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        Timer.Context t = metricRegistry.timer(this.getClass().getSimpleName() + "_close").time();
        clearFinishedUploads();

        synchronized (bufferLock) {
            if(closed){
                // Already closed, nothing to do
                return;
            }
            // We stop accepting any more loads
            closed = true;
        }

        // We wait for currently pending upload requests to finish
        // New pending pendingUploads are only added while holding the bufferLock
        try {
            Futures.allAsList(pendingUploads).get();
        } catch (Exception e) {
            if (permitFailures) {
                log.warn("Part of upload failed", e);
            }else{
                throw new IOException(e);
            }
        }

        // And then make the final upload request, and close the stream
        try {
            partUpload(buffer.toByteArray(), true);
        } catch (IOException e) {
            if (permitFailures) {
                log.warn("Part of upload failed", e);
            } else {
                throw new IOException(e);
            }
        }

        t.stop();
    }


    /**
     * Clear finished part upload requests from the queue if they are done.
     * This is necessary to prevent memory leak.
     * Also serves as a fail-fast mechanism (if {@link #permitFailures} = false)
     *
     * @throws java.io.IOException
     */
    private void clearFinishedUploads() throws IOException {
        for (ListenableFuture<Void> upload : pendingUploads) {
            if (upload.isDone()) {
                if (pendingUploads.remove(upload)) {
                    // We acquired this one
                    try {
                        upload.get();
                    } catch (Exception e) {
                        if (permitFailures) {
                            log.warn("Part of upload failed", e);
                        } else {
                            throw new IOException(e);
                        }
                    }

                }
            }
        }
    }


    private void flushAndComplete(MultipartUploadState current) throws IOException {
        MultipartUploadState newHandle = openNewFile(bucketName, prefix, newFileName());
        boolean iset = uploadState.compareAndSet(current, newHandle);
        if (iset) {
            // We set a new state, therefore we need to completeUpload the old one
            issueCompleteUploadRequest(current);
            metricRegistry.meter(this.getClass().getSimpleName()+"_flush_and_complete").mark(1);
        } else {
            // Somebody else has already initialized a new one while we were setting one up. Discard the one we setup
            issueAbortUploadRequest(newHandle);
        }
    }

    private MultipartUploadState openNewFile(String bucketName, String prefix, String filename) throws IOException {
        InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(bucketName, prefix + filename);
        try {
            InitiateMultipartUploadResult result = s3Client.initiateMultipartUpload(req);
            log.debug("Opened new file {} {}", req.getBucketName(), req.getKey());
            metricRegistry.meter(this.getClass().getSimpleName()+"_new_file").mark(1);
            return new MultipartUploadState(bucketName, prefix, filename, fileSizeBytes, result);
        } catch (AmazonClientException ace) {
            throw new IOException(ace);
        }
    }

    private void issueAbortUploadRequest(MultipartUploadState handle) {
        try {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(handle.getBucketName(), handle.getKey(), handle.getUploadId()));
            metricRegistry.meter(this.getClass().getSimpleName()+"_abort-upload").mark();
        } catch (AmazonClientException ace) {
            // Can't really do anything if abort request fails.
            log.warn("Exception while trying to abort part upload", ace);
        }
    }

    /**
     * Schedules a part upload and registers the part upload on {@link #pendingUploads}. The part upload may complete an
     * open upload if the current upload can't accept new part uploads, or if close was specified.
     *
     * @param buffer
     * @param close
     */
    private void schedulePartUpload(final ByteArrayOutputStream buffer, final boolean close) {
        pendingUploads.add(workers.submit(new VerboseCallable<Void>() {
            @Override
            public Void doCall() throws IOException {
                final byte[] part = buffer.toByteArray();
                partUpload(part, close);
                metricRegistry.meter(this.getClass().getSimpleName()+"_part-upload-success").mark();
                metricRegistry.histogram(this.getClass().getSimpleName() + "_part-upload-success-bytes").update(part.length);
                return null;
            }
        }));
    }


    /**
     * Performs a part upload, closing and opening a new upload if necessary
     *
     * @param part
     * @param close
     * @throws java.io.IOException
     */
    private void partUpload(byte[] part, boolean close) throws IOException {
        Timer.Context t = metricRegistry.timer(this.getClass().getSimpleName()+"_part-upload").time();
        MultipartUploadState current;
        while (true) {
            current = uploadState.get();
            if (part.length <= 0) {
                // Nothing to upload
                break;
            }

            // By getting a partNum, we obtain the right to execute a part upload with this state
            PartTicket ticket = current.getNewPartUploadTicket(part.length);
            if (ticket != null) {
                try {
                    UploadPartResult result = issuePartUploadRequest(part, current, ticket);

                    // Register the ETag of the part upload result
//                    checkArgument(ticket.partNum == result.getPartNumber());
                    current.reportPartETag(ticket.partNum, result.getPartETag());
                    break;
                } catch (IOException e) {
                    // Abort upload on exception
                    if (current.fail(e)) {
                        issueAbortUploadRequest(current);
                    }
                    throw e;
                }
            } else {
                // For whatever reason, current state can't take any more part uploads.
                // Attempt to close it. If it's already closed, this is a no-op
                flushAndComplete(current);
                // Now that we (or somebody else) have flushed it, we can try again
            }
        }

        t.stop();

        // The part upload has been issued at this point
        // If close was specified, close the current upload
        if (close) {
            Timer.Context t2 = metricRegistry.timer(this.getClass().getSimpleName()+"_complete").time();
            checkState(uploadState.compareAndSet(current, null));
            issueCompleteUploadRequest(current);
            t2.stop();
        }
    }

    private UploadPartResult issuePartUploadRequest(byte[] part, MultipartUploadState current, PartTicket ticket) throws IOException {
        InputStream is = new ByteArrayInputStream(part);
        UploadPartRequest uploadReq =
                new UploadPartRequest()
                        .withBucketName(current.getBucketName())
                        .withKey(current.getKey())
                        .withUploadId(current.getUploadId())
                        .withPartNumber(ticket.partNum)
                        .withInputStream(is)
                        .withPartSize(part.length)
                ;

        if (enableCheckSum) {
            Timer.Context t = metricRegistry.timer(this.getClass().getSimpleName()+"_compute_hash").time();
            String base64encodedMD5 = DatatypeConverter.printBase64Binary(Hashing.md5().hashBytes(part).asBytes());
            uploadReq.setMd5Digest(base64encodedMD5);
            t.stop();
        }


        try {
            return s3Client.uploadPart(uploadReq);
        } catch (AmazonClientException ace) {
            throw new IOException(ace);
        }
    }


    private String newFileName() {
        StringBuilder sb = new StringBuilder();
        sb.append("part_");
        sb.append(UUID.randomUUID().toString().replace("-", ""));
        return sb.toString();
    }


    private void issueCompleteUploadRequest(MultipartUploadState old) throws IOException {
        try {
            List<PartETag> list = old.closeAndWaitForETags().get(partUploadTimeoutNs, TimeUnit.NANOSECONDS);
            if (list.size() == 0) {
                // No part download in this state. Just abort the handle
                issueAbortUploadRequest(old);
                return;
            }
            final CompleteMultipartUploadRequest completeReq =
                    new CompleteMultipartUploadRequest(old.getBucketName(), old.getKey(), old.getUploadId(), list);

            log.debug("About to attempt complete {}", old);
            s3Client.completeMultipartUpload(completeReq);
            metricRegistry.meter("completeUpload-upload-success").mark();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interruption request received, aborting current upload", e);
            issueAbortUploadRequest(old);
            throw Throwables.propagate(e);
        } catch (Exception e) {
            log.error("Aborting upload due to exception :" + old, e);
            issueAbortUploadRequest(old);
            IOException r = e instanceof IOException ? (IOException)e : new IOException(e);
            throw r;
        }
    }

}
