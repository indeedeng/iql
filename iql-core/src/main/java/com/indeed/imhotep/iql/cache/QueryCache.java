package com.indeed.imhotep.iql.cache;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import com.indeed.imhotep.iql.GroupStats;

public interface QueryCache {

    /**
     * Returns whether the HDFS cache is available.
     */
    public boolean isEnabled();

    /**
     * Returns whether the cache was requested to be enabled in the app config.
     */
    public boolean isEnabledInConfig();

    public boolean isFileCached(String fileName);

    public void saveResultFromFile(String cachedFileName, File localFile) throws IOException;

    public void saveResult(String cachedFileName,
                           Iterator<GroupStats> groupStats,
                           boolean csv) throws IOException;
    
    public int sendResult(OutputStream outputStream, 
                          String fileName, 
                          int rowLimit,
                          boolean eventStream) throws IOException;

    public void healthcheck() throws IOException;

}