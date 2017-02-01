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


import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.IOUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3Uploader {

    private final AmazonS3 client;

    public S3Uploader(final AmazonS3 client) {
        this.client = client;
    }

    public String publicUpload(final S3UploadPackage uploadPackage) {
        final ObjectMetadata metadata = S3Utils.createS3MetadataForUploadPackage(uploadPackage);
        final S3ObjectCoordinates objectCoordinates = uploadPackage.getObjectCoordinates();
        InputStream byteStream = null;
        try {
            byteStream = uploadPackage.getInputStream();
            createBucketIfNonExistent(objectCoordinates.getBucketName());
            client.putObject(new PutObjectRequest(objectCoordinates.getBucketName(),
                    objectCoordinates.getObjectPath(),
                    byteStream,
                    metadata)
                            .withCannedAcl(CannedAccessControlList.PublicRead));
            return getHttpUrlToFile(objectCoordinates.getBucketName(), objectCoordinates.getObjectPath());
        } catch (final IOException e) {
            throw new UncheckedAwsS3Exception(e);
        } finally {
            IOUtils.closeQuietly(byteStream);
        }
    }

    public String getHttpUrlToFile(final String bucketName, final String fileName)
            throws MalformedURLException {
        final URL secureUrl = client.getUrl(bucketName, fileName);
        return new URL("http", secureUrl.getHost(), secureUrl.getFile()).toString();
    }

    public void createBucketIfNonExistent(final String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
        }
    }

}
