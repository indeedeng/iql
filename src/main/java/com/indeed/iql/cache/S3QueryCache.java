/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.iql.cache;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Region;

import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author darren
 */

public class S3QueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(S3QueryCache.class);

    private AmazonS3Client client;
    private String bucket;

    public S3QueryCache(PropertyResolver props) {
        String awsRegion;
        String awsKey;
        String awsSecret;

        bucket = props.getProperty("query.cache.s3.bucket", String.class);
        awsKey = props.getProperty("query.cache.s3.s3key", String.class);
        awsSecret = props.getProperty("query.cache.s3.s3secret", String.class);
        if (awsKey == null || awsSecret == null) {
            log.warn("No AWS key or Secret found.  Using Anonymous access.");
            client = new AmazonS3Client();
        } else {
            client = new AmazonS3Client(new BasicAWSCredentials(awsKey, awsSecret));
        }

        boolean exists = client.doesBucketExist(bucket);
        if (! exists) {
            awsRegion = props.getProperty("aws.s3.region",
                                          String.class,
                                          Region.US_Standard.toString());
            client.createBucket(bucket, awsRegion);
        }
    }

    /**
     * Returns whether the cache is available.
     */
    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * Returns whether the cache was requested to be enabled in the app config.
     */
    @Override
    public boolean isEnabledInConfig() {
        return true;
    }

    @Override
    public boolean isFileCached(String fileName) {
        try {
            ObjectListing objs;
            objs = client.listObjects(bucket, fileName);
            return ! objs.getObjectSummaries().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    @Nullable
    public InputStream getInputStream(String cachedFileName) throws IOException {
        try {
            return client.getObject(bucket, cachedFileName).getObjectContent();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public CompletableOutputStream getOutputStream(final String cachedFileName) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        // Wrap the returned OutputStream so that we can write to buffer and do actual write on close()
        return new CompletableOutputStream() {
            private boolean closed = false;

            @Override
            public void write(byte[] b) throws IOException {
                os.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                os.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                os.flush();
            }

            @Override
            public void write(int b) throws IOException {
                os.write(b);
            }

            @Override
            public void close() throws IOException {
                if(closed) {
                    return;
                }
                closed = true;
                os.close();

                if (completed) {
                    // do actual write
                    final byte[] csvData = os.toByteArray();
                    final ByteArrayInputStream is = new ByteArrayInputStream(csvData);
                    final ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(csvData.length);
                    client.putObject(bucket, cachedFileName, is, metadata);
                }
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        client.putObject(bucket, cachedFileName, localFile);
    }

    /**
     * Tries to see if the S3 is accessible by listing the files in the bucket
     * @throws IOException
     */
    @Override
    public void healthcheck() throws IOException {
        client.listObjects(bucket);
    }
}
