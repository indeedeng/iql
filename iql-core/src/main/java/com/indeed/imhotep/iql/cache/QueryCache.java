package com.indeed.imhotep.iql.cache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface QueryCache {

    /**
     * Returns whether the cache is available.
     */
    public boolean isEnabled();

    /**
     * Returns whether the cache was requested to be enabled in the app config.
     */
    public boolean isEnabledInConfig();

    public boolean isFileCached(String fileName);

    /**
     * Returns InputStream that can be used to read data in the cache.
     * close() should be called when done.
     */
    public InputStream getInputStream(String cachedFileName) throws IOException;

    /**
     * Returns OutputStream that can be written to to store data in the cache.
     * close() on the OutputStream MUST be called when done.
     * Note that for S3 cache this actually writes to a memory buffer first, so large data should be uploaded with writeFromFile().
     * @param cachedFileName Name of the file to upload to
     */
    public OutputStream getOutputStream(String cachedFileName) throws IOException;

    /**
     * Better optimized than getOutputStream when whole data is available in a file.
     * @param cachedFileName Name of the file to upload to
     * @param localFile Local File instance to upload from
     */
    public void writeFromFile(String cachedFileName, File localFile) throws IOException;

    public void healthcheck() throws IOException;

}