/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.iql.cache;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.web.KerberosUtils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author vladimir
 */

public class HDFSQueryCache implements QueryCache {
    static final Logger log = Logger.getLogger(HDFSQueryCache.class);

    private boolean enabled;
    private FileSystem hdfs;
    private Path cachePath;
    private boolean cacheDirWorldWritable;

    public HDFSQueryCache(PropertyResolver props) {

        enabled = true;
        try {
            kerberosLogin(props);

            cachePath = new Path(props.getProperty("query.cache.hdfs.path"));
            hdfs = cachePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
            cacheDirWorldWritable = props.getProperty("query.cache.worldwritable", Boolean.class);

            makeSurePathExists(cachePath);
        } catch (Exception e) {
            log.info("Failed to initialize the HDFS query cache. Caching disabled.", e);
            enabled = false;
        }
    }

    private void kerberosLogin(PropertyResolver props) {
        try {
            KerberosUtils.loginFromKeytab(props.getProperty("kerberos.principal"), props.getProperty("kerberos.keytab"));
        } catch (IOException e) {
            log.error("Failed to log in to Kerberos", e);
        }
    }

    /**
     * Returns whether the HDFS cache is available.
     */
    @Override
    public boolean isEnabled() {
        return enabled;
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
        if(!enabled) {
            return false;
        }
        try {
            final Path f = new Path(cachePath, fileName);
            return hdfs.exists(f);
        } catch (Exception e) {
            return false;
        }

    }

    @Override
    public InputStream getInputStream(String cachedFileName) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data from HDFS cache as it is disabled");
        }
        final Path f = new Path(cachePath, cachedFileName);
        return hdfs.open(f);
    }

    @Override
    public OutputStream getOutputStream(String cachedFileName) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to HDFS cache as it is disabled");
        }
        makeSurePathExists(cachePath);
        final Path filePath = new Path(cachePath, cachedFileName);
        final Path tempPath = new Path(filePath.toString() + "." + (System.currentTimeMillis() % 100000) + ".tmp");

        final FSDataOutputStream fileOut = hdfs.create(tempPath);
        // Wrap the returned OutputStream so that we can finish when it is closed
        return new OutputStream() {
            private boolean closed = false;

            @Override
            public void write(byte[] b) throws IOException {
                fileOut.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                fileOut.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                fileOut.flush();
            }

            @Override
            public void write(int b) throws IOException {
                fileOut.write(b);
            }

            @Override
            public void close() throws IOException {
                if(closed) {
                    return;
                }
                closed = true;
                fileOut.close();

                // Move to the final file location
                hdfs.rename(tempPath, filePath);
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        final OutputStream cacheStream = getOutputStream(cachedFileName);
        final InputStream fileIn = new BufferedInputStream(new FileInputStream(localFile));
        ByteStreams.copy(fileIn, cacheStream);
        fileIn.close();
        cacheStream.close();
    }

    private void makeSurePathExists(Path path) throws IOException {
        if(!hdfs.exists(path)) {
            hdfs.mkdirs(cachePath);
            if(cacheDirWorldWritable) {
                hdfs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx"));
            }
        }
    }

    /**
     * Tries to see if the HDFS is accessible by checking if the root cache dir exists and recreating if necessary.
     * @throws IOException
     */
    @Override
    public void healthcheck() throws IOException {
        if(!enabled) {
            throw new IllegalStateException("HDFS cache is not available");
        }

        makeSurePathExists(cachePath);
    }
}
