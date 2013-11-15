package com.peerindex.s3stream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class S3StreamFactoryTest {
    private static final String MOCK_UPLOAD_ID = "upload";

    @Test
    public void noWrites() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        AmazonS3Client client = mockClient();

        final S3StreamFactory subject = new S3StreamFactory(metricRegistry, client, TimeUnit.SECONDS, 300, 30 * 1024 * 1024, 11 * 1024 * 1024, false, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        stream.close();

        Matcher<InitiateMultipartUploadRequest> initReqMatcher = matchInitReq();

        Matcher<AbortMultipartUploadRequest> abortReqMatcher = matchAbortReq();

        verify(client, times(1)).initiateMultipartUpload(argThat(initReqMatcher));
        verify(client, times(1)).abortMultipartUpload(argThat(abortReqMatcher));
        verifyNoMoreInteractions(client);
    }

    @Test
    public void smallFile() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 30 * 1024 * 1024, 11 * 1024 * 1024, false, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        try {
            stream.write("AA");
        } finally {
            stream.close();

        }

        Matcher<UploadPartRequest> matcher = matchUploadPart("AA");

        verify(cl, times(1)).initiateMultipartUpload(argThat(matchInitReq()));
        verify(cl, times(1)).uploadPart(argThat(matcher));
        verify(cl, times(1)).completeMultipartUpload(argThat(matchCompleteReq("test", "test/")));
        verifyNoMoreInteractions(cl);
    }


    @Test
    public void twoFiles() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 10 * 1024 * 1024, 5 * 1024 * 1024, false, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        try {
            String row = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            for (int i = 0; i < 300000; i++) {
                stream.write(row);
            }
        } finally {
            stream.close();

        }

        Matcher<UploadPartRequest> matcher = matchUploadPart('A');

        verify(cl, times(2)).initiateMultipartUpload(argThat(matchInitReq()));
        verify(cl, times(3)).uploadPart(argThat(matcher));
        verify(cl, times(2)).completeMultipartUpload(argThat(matchCompleteReq("test", "test/")));
        verifyNoMoreInteractions(cl);
    }


    @Test
    public void failOnFailure() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockFailingClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 10 * 1024 * 1024, 5 * 1024 * 1024, false, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);

        try {
            String row = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            for (int i = 0; i < 300000; i++) {
                stream.write(row);
            }
            fail();
        } catch (IOException e) {
            assertEquals("simulated failure", Throwables.getRootCause(e).getMessage());
        }


        try {
            stream.close();
            fail();
        } catch (IOException e) {
            assertTrue(true);
        }

        Matcher<UploadPartRequest> matcher = matchUploadPart('A');

        verify(cl, times(2)).initiateMultipartUpload(argThat(matchInitReq()));
        verify(cl, times(1)).uploadPart(argThat(matcher));
        verify(cl, times(2)).abortMultipartUpload(argThat(matchAbortReq()));
        verifyNoMoreInteractions(cl);
    }


    @Test
    public void continueOnFailure() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockFailingClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 10 * 1024 * 1024, 5 * 1024 * 1024, true, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        try {
            String row = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            for (int i = 0; i < 300000; i++) {
                stream.write(row);
            }

        } finally {
            stream.close();

        }


        Matcher<UploadPartRequest> matcher = matchUploadPart('A');

        verify(cl, times(2)).initiateMultipartUpload(argThat(matchInitReq()));
        verify(cl, times(2)).uploadPart(argThat(matcher));
        verify(cl, times(2)).abortMultipartUpload(argThat(matchAbortReq()));
        verify(cl, times(1)).completeMultipartUpload(argThat(matchCompleteReq("test", "test/")));
        verifyNoMoreInteractions(cl);
    }

    @Test(expected = IOException.class)
    public void writeOnClosed() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockFailingClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 10 * 1024 * 1024, 5 * 1024 * 1024, true, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        stream.close();
        stream.write("AA");
    }

    @Test(expected = IOException.class)
    public void writeOnWrittenAndClosed() throws Exception {
        MetricRegistry fake = new MetricRegistry();
        AmazonS3Client cl = mockFailingClient();
        final S3StreamFactory subject = new S3StreamFactory(fake, cl, TimeUnit.SECONDS, 300, 10 * 1024 * 1024, 5 * 1024 * 1024, true, true, 4, 10);

        S3Stream stream = subject.newStream("test", "test/", Charsets.UTF_8);
        stream.write("AA");
        stream.close();
        stream.write("AA");
    }


    private CustomTypeSafeMatcher<AbortMultipartUploadRequest> matchAbortReq() {
        return new CustomTypeSafeMatcher<AbortMultipartUploadRequest>("") {
            @Override
            protected boolean matchesSafely(AbortMultipartUploadRequest item) {
                boolean ok = true;
                ok &= MOCK_UPLOAD_ID.equals(item.getUploadId());
                ok &= "test".equals(item.getBucketName());
                ok &= item.getKey().startsWith("test/");
                ok &= !item.getKey().endsWith("/");
                return ok;
            }
        };
    }

    private CustomTypeSafeMatcher<InitiateMultipartUploadRequest> matchInitReq() {
        return new CustomTypeSafeMatcher<InitiateMultipartUploadRequest>("") {
            @Override
            protected boolean matchesSafely(InitiateMultipartUploadRequest item) {
                boolean ok = true;
                ok &= "test".equals(item.getBucketName());
                ok &= item.getKey().startsWith("test/");
                ok &= !item.getKey().endsWith("/");
                return ok;
            }
        };
    }

    private Matcher<CompleteMultipartUploadRequest> matchCompleteReq(final String bucketName, final String prefix) {
        return new CustomTypeSafeMatcher<CompleteMultipartUploadRequest>("") {
            @Override
            protected boolean matchesSafely(CompleteMultipartUploadRequest subject) {
                boolean ok = subject.getBucketName().equals(bucketName);
                ok &= subject.getKey().startsWith(prefix);
                ok &= subject.getUploadId().equals(MOCK_UPLOAD_ID);
                return ok;
            }
        };
    }

    private Matcher<UploadPartRequest> matchUploadPart(final char c) {
        return new CustomTypeSafeMatcher<UploadPartRequest>("") {
            @Override
            protected boolean matchesSafely(UploadPartRequest subject) {
                try {
                    String read = CharStreams.toString(new InputStreamReader(subject.getInputStream(), Charsets.UTF_8));
                    for (int i = 0, n = read.length(); i < n; i++) {
                        checkState(read.charAt(i) == c);
                    }
                    return true;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        };
    }


    private Matcher<UploadPartRequest> matchUploadPart(final String content) {
        return new CustomTypeSafeMatcher<UploadPartRequest>("") {
            @Override
            protected boolean matchesSafely(UploadPartRequest subject) {
                String base64encodedMD5 = DatatypeConverter.printBase64Binary(Hashing.md5().hashBytes(content.getBytes(Charsets.UTF_8)).asBytes());
                return subject.getMd5Digest().equals(base64encodedMD5);
            }
        };
    }

    private AmazonS3Client mockFailingClient() {
        AmazonS3Client client = mock(AmazonS3Client.class, RETURNS_DEEP_STUBS);
        InitiateMultipartUploadResult retOnInit = mock(InitiateMultipartUploadResult.class, RETURNS_DEEP_STUBS);
        when(retOnInit.getUploadId()).thenReturn(MOCK_UPLOAD_ID);
        when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(retOnInit);
        UploadPartResult uploadPartResult = mock(UploadPartResult.class);
        when(client.uploadPart(any(UploadPartRequest.class))).thenThrow(new AmazonClientException("simulated failure")).thenReturn(uploadPartResult);
        return client;
    }

    private AmazonS3Client mockClient() {
        AmazonS3Client client = mock(AmazonS3Client.class, RETURNS_DEEP_STUBS);
        InitiateMultipartUploadResult retOnInit = mock(InitiateMultipartUploadResult.class, RETURNS_DEEP_STUBS);
        when(retOnInit.getUploadId()).thenReturn(MOCK_UPLOAD_ID);
        when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(retOnInit);
        return client;
    }


}
