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


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

public final class S3Utils {
    private S3Utils() {
        // Util class
    }

    /**
     * Create an S3 client with default config for given region and credentials
     */
    public static AmazonS3 createAmazonS3Client(final String regionName, final String accessKey,
            final String secretKey) {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3 client =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                        .withRegion(regionName)
                        .build();
        return client;
    }

    public static S3ObjectCoordinates parseObjectPath(final String s3ObjectPath) {
        if (s3ObjectPath == null || s3ObjectPath.length() == 0) {
            throw new IllegalArgumentException("An S3 object path must be provided");
        }
        final int lastSlashPos = s3ObjectPath.lastIndexOf('/');
        if (lastSlashPos == s3ObjectPath.length() - 1) {
            throw new IllegalArgumentException("An S3 object path must not end in '/': " + s3ObjectPath);
        }
        if (lastSlashPos < 0) {
            return new S3ObjectCoordinates("", s3ObjectPath);
        }
        final int firstSlashPos = s3ObjectPath.indexOf('/');

        return new S3ObjectCoordinates(s3ObjectPath.substring(0, firstSlashPos),
                s3ObjectPath.substring(firstSlashPos + 1));
    }

    public static ObjectMetadata createS3MetadataForUploadPackage(final S3UploadPackage uploadPackage) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(uploadPackage.getContentType());
        metadata.setContentLength(uploadPackage.getContentLength());
        return metadata;
    }

}
