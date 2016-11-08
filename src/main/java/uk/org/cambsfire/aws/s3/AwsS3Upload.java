package uk.org.cambsfire.aws.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AwsS3Upload {

    private static final String ORIGINAL_NAME_HEADER = "x-cfrs-filename";
    private static final String BUCKET_PREFIX = "uk.gov.cambsfire.sr.";

    public AmazonS3 createAmazonS3Client(final String regionName, final String accessKey,
            final String secretKey) {
        final Region region = Region.getRegion(Regions.fromName(regionName));
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client client = new AmazonS3Client(credentials);
        client.setRegion(region);

        return client;
    }

    public String uploadObject(final AmazonS3 client, final String originalFilename, final String s3ObjectPath,
            final String contentType, final InputStream byteStream, final long contentLength) {
        final ObjectMetadata metadata = createS3Metadata(originalFilename, contentType, contentLength);
        final String[] bucketAndFile = parseObjectPath(s3ObjectPath);
        final String bucketName = BUCKET_PREFIX + bucketAndFile[0];
        createBucketIfNonExistent(client, bucketName);
        writeImageToPersistentStore(client, bucketName, bucketAndFile[1], metadata, byteStream);

        return client.getUrl(bucketAndFile[0], bucketAndFile[1]).toString();
    }

    private void createBucketIfNonExistent(final AmazonS3 client, final String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName);
        }
    }

    private String[] parseObjectPath(final String s3ObjectPath) {
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

    private ObjectMetadata createS3Metadata(final String originalFilename,
            final String contentType,
            final long contentLength) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.setContentLength(contentLength);
        final Map<String, String> customProperties = new HashMap<>();
        customProperties.put(ORIGINAL_NAME_HEADER, originalFilename);
        metadata.setUserMetadata(customProperties);
        return metadata;
    }

    private void writeImageToPersistentStore(final AmazonS3 client, final String bucketName,
            final String imageKey,
            final ObjectMetadata imageMetadata,
            final InputStream objectBytes) {
        try (BufferedInputStream objectStream = new BufferedInputStream(objectBytes)) {
            client.putObject(new PutObjectRequest(bucketName,
                    imageKey,
                    objectStream,
                    imageMetadata));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
