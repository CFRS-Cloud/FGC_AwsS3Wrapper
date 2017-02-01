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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URL;
import java.util.Base64;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.management.*", "org.apache.http.conn.ssl.*", "com.amazonaws.http.conn.ssl.*",
        "javax.net.ssl.*" })
@PrepareForTest(S3Utils.class)
public class AwsS3UploadTest {
    private static final byte[] OBJECT_BYTES = new byte[] { 1, 2, 3 };

    @Rule
    public MockitoRule mockingRule = MockitoJUnit.rule();

    @Mock
    private AmazonS3 client;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @Test
    public void uploadObjectCreatesClientAndUploadsPackage() throws IOException {
        // given
        PowerMockito.spy(S3Utils.class);
        final String regionName = "region";
        final String accessKey = "key";
        final String secretKey = "secret";
        Mockito.when(S3Utils.createAmazonS3Client(regionName, accessKey, secretKey)).thenReturn(client);
        given(client.doesBucketExist(anyString())).willReturn(true);
        given(client.getUrl(anyString(), anyString())).willReturn(new URL("https://host/path"));
        // when
        final String s3ObjectPath = "bucket/path";
        final String contentType = "content/type";
        final String encodedBytes = Base64.getEncoder().encodeToString(OBJECT_BYTES);
        AwsS3Upload.uploadObject(regionName, accessKey, secretKey, s3ObjectPath, contentType, encodedBytes);

        // then
        verify(client).putObject(putObjectRequestCaptor.capture());
        final PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        assertThat(putObjectRequest.getBucketName()).isEqualTo("bucket");
        assertThat(putObjectRequest.getKey()).isEqualTo("path");

    }
}
