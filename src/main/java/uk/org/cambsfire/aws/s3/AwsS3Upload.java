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

import com.amazonaws.services.s3.AmazonS3;

/**
 * BPM entry-point suitable for calling from Javascript
 *
 * @author david.bower
 *
 */
public final class AwsS3Upload {

    private AwsS3Upload() {
        // Utility class
    }

    /**
     * String arguments provided for calling via Javascript integration.
     *
     * @param regionName
     *            The AWS region to upload to as a well known AWS region string
     * @param accessKey
     *            The AWS Access key ID
     * @param secretKey
     *            The AWS secret key for the uploading account
     * @param s3ObjectPath
     *            The path to the uploaded file of the form bucket-name/path
     * @param contentType
     *            The MIME type of the content
     * @param base64Bytes
     *            The object bytes as a Base64 encoded string
     * @return The public URL to the uploaded object
     * @throws UncheckedAwsS3Exception
     *             on error any upload error
     */
    @SuppressWarnings("PMD.UseObjectForClearerAPI")
    public static String uploadObject(final String regionName, final String accessKey,
            final String secretKey, final String s3ObjectPath,
            final String contentType, final String base64Bytes) {
        final AmazonS3 client = S3Utils.createAmazonS3Client(regionName, accessKey, secretKey);
        final S3UploadPackage uploadPackage = new S3UploadPackage(s3ObjectPath, contentType, base64Bytes);
        final S3Uploader uploader = new S3Uploader(client);
        return uploader.publicUpload(uploadPackage);
    }

}
