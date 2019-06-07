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

package com.indeed.iql2.server.web.servlets;

import com.google.common.base.Throwables;
import com.indeed.imhotep.utils.tempfiles.TempFile;
import com.indeed.iql.cache.CompletableOutputStream;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql2.server.web.servlets.query.SelectQueryExecution;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * The purpose of this class is to check whether there are any tests where we have different
 * queries in our set of tests that are assigned the same cache key but yield different results.
 *
 * This is most likely to be useful if tests for different but related functionality use very minor
 * variations, maintaining maximum similarity to the other (but different tests), maximizing the
 * change of a cache key collision if we screw up the cache key computation somehow.
 */
public class CollisionCheckingQueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(CollisionCheckingQueryCache.class);
    static final CollisionCheckingQueryCache INSTANCE = new CollisionCheckingQueryCache();

    private final Map<String, String> cacheHashToResultsHash = new HashMap<>();

    private CollisionCheckingQueryCache() {
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isEnabledInConfig() {
        return false;
    }

    @Override
    public boolean isFileCached(final String fileName) {
        return false;
    }

    @Override
    public InputStream getInputStream(final String cachedFileName) {
        return null;
    }

    @Override
    public CompletableOutputStream getOutputStream(final String cachedFileName) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        return new CompletableOutputStream() {
            public void write(final int b) {
                byteArrayOutputStream.write(b);
            }

            public void write(final byte[] b, final int off, final int len) {
                byteArrayOutputStream.write(b, off, len);
            }

            public void writeTo(final OutputStream out) throws IOException {
                byteArrayOutputStream.writeTo(out);
            }

            public void reset() {
                byteArrayOutputStream.reset();
            }

            public void write(final byte[] b) throws IOException {
                byteArrayOutputStream.write(b);
            }

            public void flush() throws IOException {
                byteArrayOutputStream.flush();
            }

            @Override
            public void complete() {
                observed(
                        cachedFileName,
                        computeResultHash(byteArrayOutputStream.toByteArray())
                );
            }
        };
    }

    @Override
    public void writeFromFile(final String cachedFileName, final TempFile localFile) throws IOException {
        final byte[] bytes = Files.readAllBytes(localFile.unsafeGetPath());
        observed(
                cachedFileName,
                computeResultHash(bytes)
        );
    }

    private String computeResultHash(final byte[] bytes) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (final NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }

        sha1.update(bytes);
        return Base64.encodeBase64String(sha1.digest());
    }

    private void observed(final String cacheFileName, final String resultHash) {
        if (cacheFileName.endsWith(SelectQueryExecution.METADATA_FILE_SUFFIX)) {
            return;
        }
        final String oldResult = cacheHashToResultsHash.put(cacheFileName, resultHash);
        if ((oldResult != null) && !resultHash.equals(oldResult)) {
            log.error("Observed multiple different results for the same cache key hash in test! Hash = " + cacheFileName);
            System.exit(1);
        }
    }

    @Override
    public void healthcheck() {

    }
}
