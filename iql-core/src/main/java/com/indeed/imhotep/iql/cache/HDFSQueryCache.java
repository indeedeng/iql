package com.indeed.imhotep.iql.cache;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.iql.GroupStats;
import com.indeed.imhotep.iql.IQLQuery;
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
import java.util.Iterator;

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
    public int sendResult(OutputStream outputStream, String fileName, int rowLimit, boolean eventStream) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data from HDFS cache as it is disabled");
        }
        final Path f = new Path(cachePath, fileName);
        final InputStream is = hdfs.open(f);
        return IQLQuery.copyStream(is, outputStream, rowLimit, eventStream);
    }

    @Override
    public void saveResultFromFile(String cachedFileName, File localFile) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to HDFS cache as it is disabled");
        }
        makeSurePathExists(cachePath);
        final Path filePath = new Path(cachePath, cachedFileName);
        final Path tempPath = new Path(filePath.toString() + "." + (System.currentTimeMillis() % 100000) + ".tmp");

        final InputStream fileIn = new BufferedInputStream(new FileInputStream(localFile));
        final FSDataOutputStream fileOut = hdfs.create(tempPath);
        ByteStreams.copy(fileIn, fileOut);
        fileIn.close();
        fileOut.close();

        hdfs.rename(tempPath, filePath);
    }

    @Override
    public void saveResult(String cachedFileName, Iterator<GroupStats> groupStats, boolean csv) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Can't send data to HDFS cache as it is disabled");
        }
        makeSurePathExists(cachePath);
        final Path filePath = new Path(cachePath, cachedFileName);
        final Path tempPath = new Path(filePath.toString() + "." + (System.currentTimeMillis() % 100000) + ".tmp");

        final FSDataOutputStream fileOut = hdfs.create(tempPath);
        IQLQuery.writeRowsToStream(groupStats, fileOut, csv, Integer.MAX_VALUE, false);
        fileOut.close();

        hdfs.rename(tempPath, filePath);
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
