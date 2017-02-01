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

import java.io.IOException;
import java.util.Base64;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class S3UploadPackageTest {

    @Test
    public void s3UploadPackageConvertsBase64ToBytes() throws IOException {
        // given
        final S3ObjectCoordinates objectCoordinates = new S3ObjectCoordinates("bucketName", "objectPath");
        final byte[] inputBytes = new byte[] { 1, 1, 1 };
        final String base64Content = Base64.getEncoder().encodeToString(inputBytes);

        // when
        final S3UploadPackage uploadPackage = new S3UploadPackage(objectCoordinates, "contentType", base64Content);

        // then
        final byte[] decodedBytes = new byte[3];
        IOUtils.readFully(uploadPackage.getInputStream(), decodedBytes);
        assertThat(decodedBytes).containsExactly(inputBytes);
        assertThat(uploadPackage.getContentLength()).isEqualTo(3);
    }

    @Test
    public void s3UploadPackageConvertsObjectPathNameToCoordinates() {
        // given
        final byte[] inputBytes = new byte[] { 1, 1, 1 };
        final String base64Content = Base64.getEncoder().encodeToString(inputBytes);

        // when
        final S3UploadPackage uploadPackage = new S3UploadPackage("bucket/path", "contentType", base64Content);

        // then
        final S3ObjectCoordinates objectCoordinates = uploadPackage.getObjectCoordinates();
        assertThat(objectCoordinates.getBucketName()).isEqualTo("bucket");
        assertThat(objectCoordinates.getObjectPath()).isEqualTo("path");

    }
}
