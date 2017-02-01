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

import org.junit.Test;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3UtilsTest {
    @Test(expected = IllegalArgumentException.class)
    public void parseObjectPathMustBeProvided() {
        S3Utils.parseObjectPath("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseObjectPathMustNotEndInSlash() {
        S3Utils.parseObjectPath("bucket/path/");
    }

    @Test
    public void parseObjectPathWithNoSlashGivesEmptyBucketName() {
        final S3ObjectCoordinates parsedObjectPath = S3Utils.parseObjectPath("path");
        assertThat(parsedObjectPath.getObjectPath()).as("Object path").isEqualTo("path");
        assertThat(parsedObjectPath.getBucketName()).as("Bucket name").isEmpty();
    }

    @Test
    public void parseObjectPathWithSlashGivesBucketNameAsTextBeforeSlash() {
        final S3ObjectCoordinates parsedObjectPath = S3Utils.parseObjectPath("bucket/path");
        assertThat(parsedObjectPath.getObjectPath()).as("Object path").isEqualTo("path");
        assertThat(parsedObjectPath.getBucketName()).as("Bucket name").isEqualTo("bucket");
    }

    @Test
    public void parseObjectPathWithSlashGivesBucketNameAndFolders() {
        final S3ObjectCoordinates parsedObjectPath = S3Utils.parseObjectPath("bucket/folder/path");
        assertThat(parsedObjectPath.getObjectPath()).as("Object path").isEqualTo("folder/path");
        assertThat(parsedObjectPath.getBucketName()).as("Bucket name").isEqualTo("bucket");
    }

    @Test
    public void createS3MetadataAddsContentTypeAndLength() {
        final String contentType = "content/type";
        final byte[] content = new byte[] { 0 };
        final S3UploadPackage uploadPackage =
                new S3UploadPackage(new S3ObjectCoordinates("bucket", "path"), contentType, content);
        final ObjectMetadata metadata = S3Utils.createS3MetadataForUploadPackage(uploadPackage);

        assertThat(metadata.getContentType()).isEqualTo(contentType);
        assertThat(metadata.getContentLength()).isEqualTo(content.length);

    }
}
