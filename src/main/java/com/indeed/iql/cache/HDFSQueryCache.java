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

import com.indeed.iql.web.KerberosUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author vladimir
 */

public class HDFSQueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(HDFSQueryCache.class);

    private final FileSystem hdfs;
    private final Path cachePath;
    private final boolean cacheDirWorldWritable;

    public HDFSQueryCache(PropertyResolver props) throws IOException {
        kerberosLogin(props);

        cachePath = new Path(props.getProperty("query.cache.hdfs.path"));
        hdfs = cachePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        cacheDirWorldWritable = props.getProperty("query.cache.worldwritable", Boolean.class);

        makeSurePathExists(cachePath);
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
        try {
            final Path f = new Path(cachePath, fileName);
            return hdfs.exists(f);
        } catch (Exception e) {
            return false;
        }

    }

    @Override
    @Nullable
    public InputStream getInputStream(String cachedFileName) throws IOException {
        final Path f = new Path(cachePath, cachedFileName);
        try {
            return hdfs.open(f);
        } catch (FileNotFoundException e) {
            return null;
        } catch (Exception e) {
            log.error("Error while reading from HDFS cache", e);
            return null;
        }
    }

    @Override
    public CompletableOutputStream getOutputStream(String cachedFileName) throws IOException {
        makeSurePathExists(cachePath);
        final Path filePath = new Path(cachePath, cachedFileName);
        final Path tempPath = new Path(filePath.toString() + "." + (System.currentTimeMillis() % 100000) + ".tmp");

        final FSDataOutputStream fileOut = hdfs.create(tempPath);
        // Wrap the returned OutputStream so that we can finish when it is closed
        return new CompletableOutputStream() {
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
                boolean renamed = false;
                try {
                    closed = true;
                    fileOut.close();

                    if (completed) {
                        // Move to the final file location
                        hdfs.rename(tempPath, filePath);
                        renamed = true;
                    }
                } finally {
                    if (!renamed) {
                        // "If the file does not exist the filesystem state does not change"
                        hdfs.delete(tempPath, false);
                    }
                }
            }
        };
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
        makeSurePathExists(cachePath);
    }
}
