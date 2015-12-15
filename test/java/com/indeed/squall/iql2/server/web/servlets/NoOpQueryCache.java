package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.squall.iql2.server.web.cache.QueryCache;

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
