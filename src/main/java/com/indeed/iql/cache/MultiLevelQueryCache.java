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

import com.google.common.io.ByteStreams;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author vladimir
 */

public class MultiLevelQueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(MultiLevelQueryCache.class);

    private QueryCache primaryCache;
    private QueryCache largeDataCache;
    private final long maxFileSizeForPrimaryCacheBytes;
    private static final int[] MARK_OF_LARGE_DATA_CACHE_USED = new int[] {0xFF, 0xFF, 0xFF, 0xFF};

    public MultiLevelQueryCache(QueryCache primaryCache, QueryCache largeDataCache, PropertyResolver props) {
        this.primaryCache = primaryCache;
        this.largeDataCache = largeDataCache;
        this.maxFileSizeForPrimaryCacheBytes = props.getProperty("query.cache.max.size.primary.bytes", Long.class);
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
        return primaryCache.isFileCached(fileName);
    }

    @Override
    public InputStream getInputStream(String cachedFileName) throws IOException {
        final InputStream inputStream = primaryCache.getInputStream(cachedFileName);
        if(inputStream == null) {
            return null;
        }
        if(!inputStream.markSupported()) {
            log.warn("Primary cache has to return InputStreams with markSupported returning true");
            return null;
        }
        inputStream.mark(MARK_OF_LARGE_DATA_CACHE_USED.length);
        for(int possibleMarkByte : MARK_OF_LARGE_DATA_CACHE_USED) {
            if (inputStream.read() != possibleMarkByte) {
                inputStream.reset();
                return inputStream;
            }
        }
        // the data is cached but in the large data cache
        inputStream.close();
        return largeDataCache.getInputStream(cachedFileName);
    }

    @Override
    public OutputStream getOutputStream(String cachedFileName) throws IOException {
        final ByteArrayOutputStream inMemoryCacheStream = new ByteArrayOutputStream(1000);

        return new OutputStream() {
            private long bytesBuffered = 0;
            private boolean overflowed = false;
            private OutputStream outputStream = inMemoryCacheStream;

            @Override
            public void write(int b) throws IOException {
                outputStream.write(b);
                incrementAndCheckPrimaryLimit(1);
            }

            @Override
            public void write(byte[] b) throws IOException {
                outputStream.write(b);
                incrementAndCheckPrimaryLimit(b.length);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                incrementAndCheckPrimaryLimit(len);
            }

            @Override
            public void flush() throws IOException {
                outputStream.flush();
            }

            private void incrementAndCheckPrimaryLimit(int length) throws IOException {
                if (overflowed) {
                    return;
                }
                bytesBuffered += length;
                if(bytesBuffered > maxFileSizeForPrimaryCacheBytes) {
                    overflowed = true;
                    // write a mark to the primary cache to know that we have data in the secondary cache
                    try (final OutputStream primaryCacheOutputStream = primaryCache.getOutputStream(cachedFileName)) {
                        for(int markByte: MARK_OF_LARGE_DATA_CACHE_USED) {
                            primaryCacheOutputStream.write(markByte);
                        }
                    }

                    // copy all cached data from in memory buffer to the large data cache
                    final OutputStream largeDataCacheOutputStream = largeDataCache.getOutputStream(cachedFileName);
                    largeDataCacheOutputStream.write(inMemoryCacheStream.toByteArray());
                    inMemoryCacheStream.close();
                    // future writes will go directly to the large data cache and bypass the memory buffer
                    outputStream = largeDataCacheOutputStream;
                }
            }

            @Override
            public void close() throws IOException {
                if (!overflowed) {
                    try(final OutputStream cacheStream = primaryCache.getOutputStream(cachedFileName)) {
                        cacheStream.write(inMemoryCacheStream.toByteArray());
                    }
                }
                outputStream.close();
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        try(final OutputStream cacheStream = getOutputStream(cachedFileName)) {
            try (final InputStream fileIn = new BufferedInputStream(new FileInputStream(localFile))) {
                ByteStreams.copy(fileIn, cacheStream);
            }
        }
    }

    @Override
    public void healthcheck() throws IOException {
        primaryCache.healthcheck();
        largeDataCache.healthcheck();
    }
}
