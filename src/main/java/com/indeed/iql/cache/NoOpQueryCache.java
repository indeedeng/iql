package com.indeed.iql.cache;

import java.io.File;
import java.io.InputStream;

/**
 * Query cache that doesn't do anything
 */
public class NoOpQueryCache implements QueryCache {

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
    public InputStream getInputStream(String cachedFileName) {
        return null;
    }

    @Override
    public CompletableOutputStream getOutputStream(String cachedFileName) {
        throw new IllegalStateException("Can't send data to cache as it is disabled");
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) {
        throw new IllegalStateException("Can't send data to cache as it is disabled");
    }

    @Override
    public void healthcheck() {
        throw new IllegalStateException("Cache is not available");
    }

}