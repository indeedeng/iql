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
 package com.indeed.iql1.iql.cache;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Region;

import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author darren
 */

public class S3QueryCache implements QueryCache {
    static final Logger log = Logger.getLogger(S3QueryCache.class);

    private boolean enabled;
    private AmazonS3Client client;
    private String bucket;

    public S3QueryCache(PropertyResolver props) {
        String awsRegion;
        String awsKey;
        String awsSecret;
        
        enabled = true;
        try {
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
        } catch (Exception e) {
            log.info("Failed to initialize the S3 client. Caching disabled.", e);
            enabled = false;
        }
    }

    /**
     * Returns whether the cache is available.
     */
    @Override
    public boolean isEnabled() {
        return enabled;
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
        if(!enabled) {
            return false;
        }
        try {
            ObjectListing objs;
            objs = client.listObjects(bucket, fileName);
            return ! objs.getObjectSummaries().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public InputStream getInputStream(String cachedFileName) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data from S3 cache as it is disabled");
        }
        return client.getObject(bucket, cachedFileName).getObjectContent();
    }

    @Override
    public OutputStream getOutputStream(final String cachedFileName) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to S3 cache as it is disabled");
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        // Wrap the returned OutputStream so that we can write to buffer and do actual write on close()
        return new OutputStream() {
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

                // do actual write
                byte[] csvData = os.toByteArray();
                ByteArrayInputStream is = new ByteArrayInputStream(csvData);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(csvData.length);
                client.putObject(bucket, cachedFileName, is, metadata);
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to S3 cache as it is disabled");
        }
        client.putObject(bucket, cachedFileName, localFile);
    }

    /**
     * Tries to see if the S3 is accessible by listing the files in the bucket
     * @throws IOException
     */
    @Override
    public void healthcheck() throws IOException {
        if(!enabled) {
            throw new IllegalStateException("S3 cache is not available");
        }

        client.listObjects(bucket);
    }
}
