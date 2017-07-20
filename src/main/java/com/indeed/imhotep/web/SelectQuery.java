package com.indeed.imhotep.web;

import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.imhotep.iql.IQLQuery;
import com.indeed.imhotep.iql.SelectExecutionStats;
import com.indeed.imhotep.sql.ast2.SelectStatement;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

/**
 * @author vladimir
 */

public class SelectQuery implements Closeable {
    private static final Logger log = Logger.getLogger ( SelectQuery.class );

    // this can be incremented to invalidate the old cache
    private static final byte VERSION_FOR_HASHING = 2;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private static Pattern queryTruncatePattern = Pattern.compile("\\(([^\\)]{0,100}+)[^\\)]+\\)");
    private final RunningQueriesManager runningQueriesManager;
    final String queryString;
    final String queryHash; // this hash doesn't include the shards so is different from the caching hash
    final String queryStringTruncatedForPrint;
    final String username;
    final DateTime querySubmitTimestamp;
    final SelectExecutionStats selectExecutionStats = new SelectExecutionStats();
    final SelectStatement parsedStatement;
    IQLQuery iqlQuery;
    boolean locked = false;
//    final ExecutionManager.QueryTracker queryTracker;
    DateTime queryStartTimestamp;
    final CountDownLatch waitLock = new CountDownLatch(1);
    private boolean asynchronousRelease = false;
    long id;


    public SelectQuery(RunningQueriesManager runningQueriesManager, String queryString, String username, DateTime querySubmitTimestamp, SelectStatement parsedStatement) {
        this.runningQueriesManager = runningQueriesManager;
        this.queryString = queryString;
        this.username = username;
        this.querySubmitTimestamp = querySubmitTimestamp;
        this.parsedStatement = parsedStatement;
        this.queryStringTruncatedForPrint = queryTruncatePattern.matcher(queryString).replaceAll("\\($1\\.\\.\\.\\)");
        this.queryHash = getQueryHash(queryString, null, false);
    }

    /**
     * Produces a Base64 encoded SHA-1 hash of the query and the list of shard names/versions which has to be sorted.
     */
    public static String getQueryHash(String query, @Nullable Collection<ShardIdWithVersion> shards, boolean csv) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        }
        catch(NoSuchAlgorithmException e) {
            log.error("Failed to init SHA1", e);
            throw Throwables.propagate(e);
        }
        final String standardizedQuery = query.trim().replace('"', '\'').replaceAll("\\s+", " ");
        sha1.update(standardizedQuery.getBytes(UTF8_CHARSET));
        if(shards != null) {
            for(ShardIdWithVersion shard : shards) {
                sha1.update(shard.getShardId().getBytes(UTF8_CHARSET));
                sha1.update(Longs.toByteArray(shard.getVersion()));
                sha1.update(csv ? (byte)1 : 0);
            }
        }
        sha1.update(VERSION_FOR_HASHING);
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }

    public void lock() {
        if (!runningQueriesManager.isEnabled()) {
            return;
        }

        final long waitStartTime = System.currentTimeMillis();
        runningQueriesManager.register(this);
        // block until inserted into mysql
        try {
            // TODO: handle MySQL failure to avoid blocking forever?
            waitLock.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        final long queryStartedTimestamp = System.currentTimeMillis();
        selectExecutionStats.setPhase("lockWaitMillis", queryStartedTimestamp-waitStartTime);
    }

    public void close() {
        if (!runningQueriesManager.isEnabled()) {
            return;
        }

        if (locked) {
            try {
                // delete from DB
                runningQueriesManager.release(this);
            } catch (Exception e) {
                log.error("Failed to release the query tracking for " + queryStringTruncatedForPrint);
            }

        }
    }

//     TODO: rework since query may be killed from another daemon
    public void kill() {
        try {
            iqlQuery.close();
        } catch (Exception e) {
            log.warn("Failed to close the Imhotep session to kill query" + queryStringTruncatedForPrint, e);
        }
    }

    public void onStarted(long id, DateTime startedTimestamp) {
        this.id = id;
        this.queryStartTimestamp = startedTimestamp;
        this.locked = true;
        waitLock.countDown();
    }

    public void markAsynchronousRelease() {
        this.asynchronousRelease = true;
    }

    public boolean isAsynchronousRelease() {
        return asynchronousRelease;
    }

    @Override
    public String toString() {
        return "SelectQuery{" +
                "runningQueriesManager=" + runningQueriesManager +
                ", queryString='" + queryString + '\'' +
                ", queryHash='" + queryHash + '\'' +
                ", queryStringTruncatedForPrint='" + queryStringTruncatedForPrint + '\'' +
                ", username='" + username + '\'' +
                ", querySubmitTimestamp=" + querySubmitTimestamp +
                ", selectExecutionStats=" + selectExecutionStats +
                ", parsedStatement=" + parsedStatement +
                ", iqlQuery=" + iqlQuery +
                ", locked=" + locked +
                ", queryStartTimestamp=" + queryStartTimestamp +
                ", waitLock=" + waitLock +
                ", asynchronousRelease=" + asynchronousRelease +
                ", id=" + id +
                '}';
    }
}
