package com.indeed.imhotep.iql.cache;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.xml.bind.PropertyException;

import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import com.indeed.imhotep.iql.GroupStats;

public class QueryCacheFactory {
    static final Logger log = Logger.getLogger(QueryCacheFactory.class);
    
    public static final QueryCache newQueryCache(PropertyResolver props) throws PropertyException {
        final String cacheType;
        boolean enabled;
        
        enabled = props.getProperty("query.cache.enabled", Boolean.class, true);
        if(!enabled) {
            log.info("Query caching disabled in config");
            return new NoOpQueryCache();
        }
        cacheType = props.getProperty("query.cache.backend", String.class, "HDFS");
        if ("HDFS".equals(cacheType)) {
            return new HDFSQueryCache(props);
        }
        if ("S3".equals(cacheType)) {
            return new S3QueryCache(props);
        }
        
        throw new PropertyException("Unknown cache type (property: query.cache.backend): "
                + cacheType);
    }
    
    static class NoOpQueryCache implements QueryCache {

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public boolean isEnabledInConfig() {
            return false;
        }

        @Override
        public boolean isFileCached(String fileName) {
            return false;
        }

        @Override
        public void saveResultFromFile(String cachedFileName, File localFile) throws IOException {
            throw new IllegalStateException("Can't send data to cache as it is disabled");
        }

        @Override
        public void saveResult(String cachedFileName, Iterator<GroupStats> groupStats, boolean csv) throws IOException {
            throw new IllegalStateException("Can't send data to cache as it is disabled");
        }

        @Override
        public int sendResult(OutputStream outputStream, String fileName, int rowLimit, boolean eventStream) throws IOException {
            throw new IllegalStateException("Can't send data to cache as it is disabled");
        }

        @Override
        public void healthcheck() throws IOException {
            throw new IllegalStateException("Cache is not available");
        }
        
    }

}
