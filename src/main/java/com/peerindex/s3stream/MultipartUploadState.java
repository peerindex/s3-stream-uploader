package com.peerindex.s3stream;

import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
class MultipartUploadState {
    private final InitiateMultipartUploadResult initResult;
    private final String bucketName;
    private final String prefix;
    private final String filename;
    private final long fileSizeBytes;


    private final Object lock = new Object();

    // Guarded by lock
    private boolean closed = false;
    private int partNum = 1;
    private long uploadedSoFar = 0;
    private final Map<Integer, SettableFuture<PartETag>> etTags = new ConcurrentHashMap<Integer, SettableFuture<PartETag>>();

    MultipartUploadState(String bucketName, String prefix, String filename, long fileSizeBytes, InitiateMultipartUploadResult initResult) {
        this.initResult = initResult;
        this.fileSizeBytes = fileSizeBytes;
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.filename = filename;
    }


    PartTicket getNewPartUploadTicket(long requestedSizeToUpload) {
        synchronized (lock) {
            if (!closed && uploadedSoFar + requestedSizeToUpload <= fileSizeBytes) {
                int partNumber = partNum++;
                SettableFuture<PartETag> partFuture = SettableFuture.create();
                etTags.put(partNumber, partFuture);
                uploadedSoFar += requestedSizeToUpload;
                return new PartTicket(partNumber, uploadedSoFar);
            } else {
                // No more room
                return null;
            }
        }
    }

    void reportPartETag(int part, PartETag tag) {
        etTags.get(part).set(tag);
    }

    boolean fail(Throwable cause) {
        synchronized (lock) {
            for (SettableFuture<PartETag> future : etTags.values()) {
                if (cause != null) {
                    future.setException(cause);
                } else {
                    future.cancel(true);
                }
            }
            boolean firstFailure = !closed;
            closed = true;
            return firstFailure;
        }

    }


    ListenableFuture<List<PartETag>> closeAndWaitForETags() throws IOException {
        synchronized (lock) {
            if(closed){
                throw new IOException("This stream is already closed");
            }
            closed = true;
            return Futures.allAsList(etTags.values());
        }
    }


    String getBucketName() {
        return bucketName;
    }

    String getKey() {
        return prefix + filename;
    }

    String getUploadId() {
        return initResult.getUploadId();
    }


    @Override
    public String toString() {
        synchronized (lock) {
            return "MultipartUploadState{" +
                    "initResult=" + initResult +
                    ", bucketName='" + bucketName + '\'' +
                    ", prefix='" + prefix + '\'' +
                    ", filename='" + filename + '\'' +
                    ", closed=" + closed +
                    ", partNum=" + partNum +
                    ", uploadedSoFar=" + uploadedSoFar +
                    ", etTags=" + etTags +
                    '}';
        }
    }
}
