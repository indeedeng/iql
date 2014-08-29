package com.indeed.imhotep.iql.cache;

import com.indeed.imhotep.iql.GroupStats;
import com.indeed.imhotep.iql.IQLQuery;
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
import java.util.Iterator;

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
        
        enabled = true;
        try {
            bucket = props.getProperty("query.cache.s3.bucket", String.class);
            client = new AmazonS3Client();

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
     * Returns whether the HDFS cache is available.
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
    public int sendResult(OutputStream outputStream, String fileName, int rowLimit, boolean eventStream) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data from S3 cache as it is disabled");
        }
        final InputStream is = client.getObject(bucket, fileName).getObjectContent();
        return IQLQuery.copyStream(is, outputStream, rowLimit, eventStream);
    }

    @Override
    public void saveResultFromFile(String cachedFileName, File localFile) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to S3 cache as it is disabled");
        }
        client.putObject(bucket, cachedFileName, localFile);
    }

    @Override
    public void saveResult(String cachedFileName, Iterator<GroupStats> groupStats, boolean csv) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to S3 cache as it is disabled");
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        IQLQuery.writeRowsToStream(groupStats, os, csv, Integer.MAX_VALUE, false);

        byte[] csvData = os.toByteArray();
        ByteArrayInputStream is = new ByteArrayInputStream(csvData);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(csvData.length);
        client.putObject(bucket, cachedFileName, is, metadata);
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
