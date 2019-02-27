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

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface QueryCache {

    /**
     * Returns whether the cache is available.
     */
    boolean isEnabled();

    /**
     * Returns whether the cache was requested to be enabled in the app config.
     */
    boolean isEnabledInConfig();

    boolean isFileCached(String fileName);

    /**
     * Returns InputStream that can be used to read data in the cache.
     * close() should be called when done.
     * Returns null if the file is not cached or cache is disabled.
     */
    @Nullable
    InputStream getInputStream(String cachedFileName) throws IOException;

    /**
     * Returns OutputStream that can be written to to store data in the cache.
     * close() on the OutputStream MUST be called when done.
     * Note that for S3 cache this actually writes to a memory buffer first, so large data should be uploaded with writeFromFile().
     * @param cachedFileName Name of the file to upload to
     */
    CompletableOutputStream getOutputStream(String cachedFileName) throws IOException;

    /**
     * Better optimized than getOutputStream when whole data is available in a file.
     * @param cachedFileName Name of the file to upload to
     * @param localFile Local File instance to upload from
     */
    default void writeFromFile(String cachedFileName, File localFile) throws IOException {
        try (final CompletableOutputStream cacheStream = getOutputStream(cachedFileName)) {
            try (final InputStream fileIn = new BufferedInputStream(new FileInputStream(localFile))) {
                ByteStreams.copy(fileIn, cacheStream);
            }
            cacheStream.complete();
        }
    }

    void healthcheck() throws IOException;
}