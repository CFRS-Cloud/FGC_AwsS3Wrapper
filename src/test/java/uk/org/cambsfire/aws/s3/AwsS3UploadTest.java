package uk.org.cambsfire.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URL;
import java.util.Base64;

import org.apache.commons.io.IOUtils;

/*-
 * #%L
 * AWS S3 Wrapper for BPM
 * %%
 * Copyright (C) 2016 Cambridgeshire Fire and Rescue Service
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

import org.junit.Test;

public class AwsS3UploadTest {
    @Test
    public void awsS3UploadIntegrationTest() throws Exception {
        final String awsId = System.getenv("AWS_ACCESS_KEY_ID");
        final String awsSecret = System.getenv("AWS_ACCESS_KEY_SECRET");
        final String s3BucketName = "com.lourish.testbucket2";
        final String s3ObjectPath = s3BucketName + "/public+file2.pdf";
        final String contentType = "application/pdf";
        try (final InputStream byteStream = ClassLoader.getSystemResource("test.pdf").openStream()) {
            byte[] fileBytes = IOUtils.toByteArray(byteStream);
            final String base64Bytes = Base64.getEncoder().encodeToString(fileBytes);
            fileBytes = null;
            final String objectUrlStr = AwsS3Upload.uploadObject("us-east-1",
                    awsId,
                    awsSecret,
                    s3ObjectPath,
                    contentType,
                    base64Bytes);
            final URL objectUrl = new URL(objectUrlStr);
            assertThat(objectUrl).hasProtocol("http")
                    .hasHost(s3BucketName + ".s3.amazonaws.com")
                    .hasPath("/public%2Bfile2.pdf");
        }
    }
}
