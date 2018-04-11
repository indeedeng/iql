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

package com.indeed.squall.iql2.server.web.servlets;


import com.indeed.imhotep.iql.cache.QueryCache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NoOpQueryCache implements QueryCache {
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
    public InputStream getInputStream(String cachedFileName) throws IOException {
        return null;
    }

    @Override
    public OutputStream getOutputStream(String cachedFileName) throws IOException {
        return null;
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {

    }

    @Override
    public void healthcheck() throws IOException {

    }
}
