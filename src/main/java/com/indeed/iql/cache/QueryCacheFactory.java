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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.bind.PropertyException;

import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

public class QueryCacheFactory {
    static final Logger log = Logger.getLogger(QueryCacheFactory.class);
    
    public static final QueryCache newQueryCache(PropertyResolver props) throws PropertyException {
        final String cacheType;
        boolean enabled;
        
        enabled = props.getProperty("query.cache.enabled", Boolean.class, true);
        if(!enabled) {
            log.info("Query caching disabled in config");
            return new NoOpQueryCache(false);
        }
        cacheType = props.getProperty("query.cache.backend", String.class, "HDFS");
        try {
            if ("HDFS".equalsIgnoreCase(cacheType)) {
                return new HDFSQueryCache(props);
            }
            if ("S3".equalsIgnoreCase(cacheType)) {
                return new S3QueryCache(props);
            }
            if ("redis".equalsIgnoreCase(cacheType)) {
                return new RedisQueryCache(props);
            }
            if ("redishdfs".equalsIgnoreCase(cacheType)) {
                return new MultiLevelQueryCache(new RedisQueryCache(props), new HDFSQueryCache(props), props);
            }
            if ("s3hdfs".equalsIgnoreCase(cacheType)) {
                return new MultiLevelQueryCache(new S3QueryCache(props), new HDFSQueryCache(props), props);
            }
        } catch (Exception e) {
            log.warn("Failed to initialize the query cache. Caching disabled.", e);
            return new NoOpQueryCache(true);
        }
        
        throw new PropertyException("Unknown cache type (property: query.cache.backend): "
                + cacheType);
    }
    
    static class NoOpQueryCache implements QueryCache {

        private final boolean enabledInConfig;

        public NoOpQueryCache(boolean enabledInConfig) {
            this.enabledInConfig = enabledInConfig;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public boolean isEnabledInConfig() {
            return enabledInConfig;
        }

        @Override
        public boolean isFileCached(String fileName) {
            return false;
        }

        @Override
        public InputStream getInputStream(String cachedFileName) throws IOException {
            throw new IllegalStateException("Can't read data from cache as it is disabled");
        }

        @Override
        public OutputStream getOutputStream(String cachedFileName) throws IOException {
            throw new IllegalStateException("Can't send data to cache as it is disabled");
        }

        @Override
        public void writeFromFile(String cachedFileName, File localFile) throws IOException {
            throw new IllegalStateException("Can't send data to cache as it is disabled");
        }

        @Override
        public void healthcheck() throws IOException {
            throw new IllegalStateException("Cache is not available");
        }
        
    }

}
