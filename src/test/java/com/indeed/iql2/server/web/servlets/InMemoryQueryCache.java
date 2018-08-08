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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.indeed.iql.cache.QueryCache;
import com.indeed.util.io.Files;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InMemoryQueryCache implements QueryCache {
    private final Map<String, String> cachedValues = new HashMap<>();
    private final Set<String> readsTracked = new HashSet<>();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isEnabledInConfig() {
        return true;
    }

    @Override
    public boolean isFileCached(String fileName) {
        return cachedValues.containsKey(fileName);
    }

    @Override
    public InputStream getInputStream(String cachedFileName) throws IOException {
        readsTracked.add(cachedFileName);
        return new ByteArrayInputStream(cachedValues.get(cachedFileName).getBytes());
    }

    @Override
    public OutputStream getOutputStream(final String cachedFileName) throws IOException {
        final ByteList bytes = new ByteArrayList();
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                bytes.addAll(Bytes.asList(Ints.toByteArray(b)));
            }

            @Override
            public void flush() throws IOException {
                cachedValues.put(cachedFileName, new String(bytes.toByteArray()));
            }

            @Override
            public void close() throws IOException {
                cachedValues.put(cachedFileName, new String(bytes.toByteArray()));
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        final String[] lines = Files.readTextFileOrDie(localFile.getAbsolutePath());
        cachedValues.put(cachedFileName, Joiner.on('\n').join(lines));
    }

    @Override
    public void healthcheck() throws IOException {

    }

    public Set<String> getReadsTracked() {
        return ImmutableSet.copyOf(readsTracked);
    }

    public void clearReadsTracked() {
        readsTracked.clear();
    }
}
