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

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author vladimir
 */

public class MultiLevelQueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(MultiLevelQueryCache.class);

    private final QueryCache primaryCache;
    private final QueryCache largeDataCache;
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
    public CompletableOutputStream getOutputStream(String cachedFileName) {
        final ByteArrayOutputStream inMemoryCacheStream = new ByteArrayOutputStream(1000);
        //noinspection IOResourceOpenedButNotSafelyClosed
        final DelegatingCompletableOutputStream wrappedInMemoryStream = new DelegatingCompletableOutputStream(inMemoryCacheStream);

        return new CompletableOutputStream() {
            private long bytesBuffered = 0;
            private boolean overflowed = false;
            private CompletableOutputStream outputStream = wrappedInMemoryStream;

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

                    Preconditions.checkState(outputStream == wrappedInMemoryStream);

                    // copy all cached data from in memory buffer to the large data cache
                    final CompletableOutputStream largeDataCacheOutputStream = largeDataCache.getOutputStream(cachedFileName);
                    largeDataCacheOutputStream.write(inMemoryCacheStream.toByteArray());
                    // future writes will go directly to the large data cache and bypass the memory buffer
                    outputStream.close();
                    outputStream = largeDataCacheOutputStream;
                }
            }

            @Override
            public void close() throws IOException {
                // This is really gross for a reason.
                // We want to guarantee clean up resources associated with the all caches regardless of whether we
                // successfully wrote into them.
                // We do however need to keep track of whether we successfully wrote into them.
                // The first invariant is that we only complete a successful write into the primary
                // cache if all operations up until this point, plus the final operation inside of this code succeeds.
                // The second invariant is that we only complete a successful write to the outputStream (which may
                // be the secondary cache) if all operations up to this point were successful and the write to the
                // primary cache was successful (indicated by the close() call succeeding).

                try {
                    if (completed) {
                        final CompletableOutputStream primaryCacheStream = primaryCache.getOutputStream(cachedFileName);
                        try {
                            if (overflowed) {
                                // write a mark to the primary cache to know that we have data in the secondary cache
                                for (int markByte : MARK_OF_LARGE_DATA_CACHE_USED) {
                                    primaryCacheStream.write(markByte);
                                }
                            } else {
                                // write actual data to the primary cache
                                primaryCacheStream.write(inMemoryCacheStream.toByteArray());
                            }
                            primaryCacheStream.complete();
                        } finally {
                            primaryCacheStream.close();
                            // mark outputStream as complete iff .close() succeeded and primaryCacheStream was complete
                            if (primaryCacheStream.completed) {
                                outputStream.complete();
                            }
                        }
                    }
                } finally {
                    outputStream.close();
                }
            }
        };
    }

    @Override
    public void healthcheck() throws IOException {
        primaryCache.healthcheck();
        largeDataCache.healthcheck();
    }
}
