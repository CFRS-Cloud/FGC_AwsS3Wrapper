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


import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.codec.binary.Base64;

public class S3UploadPackage {
    private final S3ObjectCoordinates objectCoordinates;
    private final String contentType;
    private final byte[] objectBytes;

    public S3UploadPackage(final S3ObjectCoordinates objectCoordinates, final String contentType,
            final byte[] content) {
        super();
        this.objectCoordinates = objectCoordinates;
        this.contentType = contentType;
        this.objectBytes = content;
    }

    public S3UploadPackage(final S3ObjectCoordinates objectCoordinates, final String contentType,
            final String base64Bytes) {
        super();
        this.objectCoordinates = objectCoordinates;
        this.contentType = contentType;
        this.objectBytes = Base64.decodeBase64(base64Bytes);
    }

    public S3UploadPackage(final String s3ObjectPath, final String contentType, final String base64Bytes) {
        this(S3Utils.parseObjectPath(s3ObjectPath), contentType, base64Bytes);
    }

    public S3ObjectCoordinates getObjectCoordinates() {
        return objectCoordinates;
    }

    public String getContentType() {
        return contentType;
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(objectBytes);
    }

    public long getContentLength() {
        return objectBytes.length;
    }

}
