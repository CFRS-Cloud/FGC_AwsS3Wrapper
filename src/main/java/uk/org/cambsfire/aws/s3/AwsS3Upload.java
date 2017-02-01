package uk.org.cambsfire.aws.s3;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * PoC of an S3 uploader to be called from Javascript in BPM
 * 
 * @author david.bower
 *
 */
public final class AwsS3Upload {

    private AwsS3Upload() {
        // Utility class
    }

    /**
     * String arguments provided for calling via Javascript integration
     */
    @SuppressWarnings("PMD.UseObjectForClearerAPI")
    public static String uploadObject(final String regionName, final String accessKey,
            final String secretKey, final String s3ObjectPath,
            final String contentType, final String base64Bytes) {
        final AmazonS3 client = createAmazonS3Client(regionName, accessKey, secretKey);
        return uploadObject(client, s3ObjectPath, contentType, base64Bytes);
    }

    private static AmazonS3 createAmazonS3Client(final String regionName, final String accessKey,
            final String secretKey) {
        final Region region = Region.getRegion(Regions.fromName(regionName));
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client client = new AmazonS3Client(credentials);
        client.setRegion(region);

        return client;
    }

    private static String uploadObject(final AmazonS3 client, final String s3ObjectPath,
            final String contentType, final String base64Bytes) {
        final byte[] objectBytes = Base64.decodeBase64(base64Bytes);
        final ObjectMetadata metadata = createS3Metadata(contentType, objectBytes.length);
        final String[] bucketAndFile = parseObjectPath(s3ObjectPath);
        final String bucketName = bucketAndFile[0];
        createBucketIfNonExistent(client, bucketName);
        try (final ByteArrayInputStream byteStream =
                new ByteArrayInputStream(objectBytes)) {
            final String fileName = bucketAndFile[1];
            writeImageToPersistentStore(client, bucketName, fileName, metadata, byteStream);
            return getHttpUrlToFile(client, bucketName, fileName);
        } catch (final IOException e) {
            throw new UncheckedAwsS3Exception(e);
        }
    }

    private static String getHttpUrlToFile(final AmazonS3 client, final String bucketName, final String fileName)
            throws MalformedURLException {
        final URL secureUrl = client.getUrl(bucketName, fileName);
        return new URL("http", secureUrl.getHost(), secureUrl.getFile()).toString();
    }

    private static void createBucketIfNonExistent(final AmazonS3 client, final String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
        }
    }

    private static String[] parseObjectPath(final String s3ObjectPath) {
        if (s3ObjectPath == null || s3ObjectPath.length() == 0) {
            throw new IllegalArgumentException("An S3 object path must be provided");
        }
        final int lastSlashPos = s3ObjectPath.lastIndexOf('/');
        if (lastSlashPos == s3ObjectPath.length() - 1) {
            throw new IllegalArgumentException("An S3 object path must not end in '/': " + s3ObjectPath);
        }
        if (lastSlashPos < 0) {
            return new String[] { "", s3ObjectPath };
        }

        return new String[] { s3ObjectPath.substring(0, lastSlashPos), s3ObjectPath.substring(lastSlashPos + 1) };
    }

    private static ObjectMetadata createS3Metadata(final String contentType,
            final long contentLength) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.setContentLength(contentLength);
        return metadata;
    }

    private static void writeImageToPersistentStore(final AmazonS3 client, final String bucketName,
            final String imageKey,
            final ObjectMetadata imageMetadata,
            final InputStream objectStream) {
        client.putObject(new PutObjectRequest(bucketName,
                imageKey,
                objectStream,
                imageMetadata)
                        .withCannedAcl(CannedAccessControlList.PublicRead));
    }
}
