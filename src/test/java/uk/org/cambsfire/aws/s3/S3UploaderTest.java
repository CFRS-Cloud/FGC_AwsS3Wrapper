package uk.org.cambsfire.aws.s3;

/*-
 * #%L
 * AWS S3 Wrapper for BPM
 * %%
 * Copyright (C) 2016 - 2017 Cambridgeshire Fire and Rescue Service
 * %%
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the Cambridgeshire Fire and Rescue Service nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3UploaderTest {
    private static final String OBJECT_CONTENT_TYPE = "contentType";
    private static final String OBJECT_PATH = "objectPath";
    private static final String BUCKET_NAME = "bucketName";
    private static final byte[] OBJECT_CONTENT_BYTES = new byte[] { 1, 2, 3 };

    @Rule
    public MockitoRule mockingRule = MockitoJUnit.rule();

    @Mock
    private AmazonS3 client;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;
    @Captor
    private ArgumentCaptor<CreateBucketRequest> createBucketRequestCaptor;

    @Test
    public void publicUploadCallsClientToUploadToExistingBucketWithPublicReadAcl() throws Exception {
        // given
        final S3Uploader uploader = new S3Uploader(client);
        given(client.getUrl(BUCKET_NAME, OBJECT_PATH)).willReturn(new URL("https://host/path"));
        given(client.doesBucketExist(BUCKET_NAME)).willReturn(true);

        // when
        final S3ObjectCoordinates objectCoordinates = new S3ObjectCoordinates(BUCKET_NAME, OBJECT_PATH);
        final S3UploadPackage uploadPackage =
                new S3UploadPackage(objectCoordinates, OBJECT_CONTENT_TYPE, OBJECT_CONTENT_BYTES);
        final String urlFromClient = uploader.publicUpload(uploadPackage);

        // then
        assertThat(new URL(urlFromClient)).hasProtocol("http").hasHost("host").hasPath("/path");
        verifyCorrectPutRequestMadeByClient();
        verify(client, never()).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void publicUploadCreatesPublicBucketIfRequired() throws Exception {
        // given
        final S3Uploader uploader = new S3Uploader(client);
        given(client.getUrl(BUCKET_NAME, OBJECT_PATH)).willReturn(new URL("https://host/path"));

        // when
        final S3ObjectCoordinates objectCoordinates = new S3ObjectCoordinates(BUCKET_NAME, OBJECT_PATH);
        final S3UploadPackage uploadPackage =
                new S3UploadPackage(objectCoordinates, OBJECT_CONTENT_TYPE, OBJECT_CONTENT_BYTES);
        final String urlFromClient = uploader.publicUpload(uploadPackage);

        // then
        assertThat(urlFromClient).isNotNull();
        verifyCorrectPutRequestMadeByClient();
        verifyCreateBucketRequestMadeByClient();
    }

    private void verifyCreateBucketRequestMadeByClient() {
        verify(client).createBucket(createBucketRequestCaptor.capture());
        final CreateBucketRequest createBucketRequest = createBucketRequestCaptor.getValue();
        assertThat(createBucketRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(createBucketRequest.getCannedAcl()).isEqualTo(CannedAccessControlList.PublicRead);
    }

    private void verifyCorrectPutRequestMadeByClient() throws IOException {
        verify(client).putObject(putObjectRequestCaptor.capture());
        final PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        assertThat(putObjectRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(putObjectRequest.getKey()).isEqualTo(OBJECT_PATH);
        assertThat(putObjectRequest.getMetadata().getContentType()).isEqualTo(OBJECT_CONTENT_TYPE);
        assertThat(putObjectRequest.getMetadata().getContentLength()).isEqualTo(OBJECT_CONTENT_BYTES.length);
        final byte[] uploadedBytes = new byte[OBJECT_CONTENT_BYTES.length];
        IOUtils.readFully(putObjectRequest.getInputStream(), uploadedBytes);
        assertThat(uploadedBytes).containsExactly(OBJECT_CONTENT_BYTES);
        assertThat(putObjectRequest.getCannedAcl()).isEqualTo(CannedAccessControlList.PublicRead);
    }
}
