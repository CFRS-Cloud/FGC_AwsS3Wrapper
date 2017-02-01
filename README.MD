# AWS S3 Upload Client

## Overview 

A basic AWS upload client intended for use as an interface between BPM and Amazon S3.

## Usage

The entry-point class is [AwsS3Upload](src/main/java/uk/org/cambsfire/aws/s3/AwsS3Upload.java) and takes the object to
store as a Base64 String (as BPM cannot interface with Java streams).

All objects are created with public read visibility and buckets are created if they do not exist.

## Build
The following command will test and build the distribution, the final jat residing in the target directory.

```
./mvnw install
```

## Distribution

The project creates a 'shaded' jar containing all dependencies 

## Testing

To run the IntegrationTest category tests two environment variables must be set for an Amazon AWS account:

* AWS_ACCESS_KEY_ID: The Access Key ID for your AWS account
* AWS_ACCESS_KEY_SECRET: The Secret Access Key for you AWS account
