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

package com.indeed.iql.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.Shard;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql2.execution.progress.ProgressCallback;
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

/**
 * @author vladimir
 */

public class SelectQuery implements Closeable {
    private static final Logger log = Logger.getLogger ( SelectQuery.class );

    public static int VERSION_FOR_HASHING = 5;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private final RunningQueriesManager runningQueriesManager;
    final QueryInfo queryInfo;
    public final String queryHash; // this hash doesn't include the shards so is different from the caching hash
    final String shortHash; // queryHash truncated
    final ClientInfo clientInfo;
    final Limits limits;
    final DateTime querySubmitTimestamp;
    final byte sessions;    // imhotep sessions
    /** Only in IQL1 */
    @Nullable
    final IQL1SelectStatement parsedStatement;
    final QueryMetadata queryMetadata;
    private final Closeable queryResourceCloser;
    private final ProgressCallback progressCallback;
    @Nullable
    RuntimeException cancellationException = null; // non-null iff query is cancelled
    DateTime queryStartTimestamp;
    private final CountDownLatch waitLock = new CountDownLatch(1);
    private boolean asynchronousRelease = false;
    long id;
    private boolean closed = false;


    public SelectQuery(
            QueryInfo queryInfo,
            RunningQueriesManager runningQueriesManager,
            String queryString,
            ClientInfo clientInfo,
            Limits limits,
            DateTime querySubmitTimestamp,
            IQL1SelectStatement parsedStatement,
            byte sessions,
            QueryMetadata queryMetadata,
            Closeable queryResourceCloser,
            ProgressCallback progressCallback
    ) {
        this.queryInfo = queryInfo;
        this.runningQueriesManager = runningQueriesManager;
        this.clientInfo = clientInfo;
        this.limits = limits;
        this.querySubmitTimestamp = querySubmitTimestamp;
        this.parsedStatement = parsedStatement;
        this.queryHash = getQueryHash(queryString, null, false);
        this.sessions = sessions;
        this.queryMetadata = queryMetadata;
        this.queryResourceCloser = queryResourceCloser;
        this.progressCallback = progressCallback;
        this.shortHash = this.queryHash.substring(0, 6);
        log.debug("Query created with hash " + shortHash);
    }

    /**
     * Produces a Base64 encoded SHA-1 hash of the query and the list of shard names/versions which has to be sorted.
     */
    public static String getQueryHash(String query, @Nullable Collection<Shard> shards, boolean csv) {
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
            for(Shard shard : shards) {
                sha1.update(shard.getShardId().getBytes(UTF8_CHARSET));
                sha1.update(Longs.toByteArray(shard.getVersion()));
                sha1.update(csv ? (byte)1 : 0);
            }
        }
        sha1.update(Ints.toByteArray(VERSION_FOR_HASHING));
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }

    public void lock() {
        final long waitStartTime = System.currentTimeMillis();
        runningQueriesManager.register(this);

        if (!runningQueriesManager.isEnabled()) {
            return;
        }

        // block until inserted into mysql
        try {
            // TODO: handle MySQL failure to avoid blocking forever?
            waitLock.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        final long queryStartedTimestamp = System.currentTimeMillis();
        queryInfo.lockWaitMillis = queryStartedTimestamp-waitStartTime;
        checkCancelled();
    }

    public void checkCancelled() {
        if (cancellationException != null) {
            throw cancellationException;
        }
    }

    public void close() {
        if (!runningQueriesManager.isEnabled()) {
            return;
        }
        if(closed) {
            return;
        }
        closed = true;

        try {
            runningQueriesManager.unregister(this);
        } catch (Exception e) {
            log.error("Failed to release the query tracking for " + queryInfo.queryStringTruncatedForPrint, e);
        }
    }

    void kill() {
        closeIqlQuery();
    }

    private void closeIqlQuery() {
        try {
            if(queryResourceCloser != null) {
                queryResourceCloser.close();
            }
        } catch (Exception e) {
            log.warn("Error while attempting to close IQL query" + queryInfo.queryStringTruncatedForPrint, e);
        }
    }

    public void onInserted(long id) {
        this.progressCallback.queryIdAssigned(id);
        this.queryInfo.queryId = id;
        this.id = id;
    }

    public void onStarted(DateTime startedTimestamp) {
        log.debug("Started query " + shortHash + " as id " + id);
        this.queryStartTimestamp = startedTimestamp;
        waitLock.countDown();
    }

    public void markAsynchronousRelease() {
        asynchronousRelease = true;
    }

    @JsonIgnore
    public boolean isAsynchronousRelease() {
        return asynchronousRelease;
    }

    public String getUsername() {
        return clientInfo.username;
    }

    public String getClient() {
        return clientInfo.client;
    }

    public String getSubmitTime() {
        return querySubmitTimestamp.toString();
    }

    @JsonIgnore
    public DateTime getQueryStartTimestamp() {
        return queryStartTimestamp;
    }

    @Override
    public String toString() {
        return "SelectQuery{" +
                "runningQueriesManager=" + runningQueriesManager +
                ", queryHash='" + queryHash + '\'' +
                ", queryStringTruncatedForPrint='" + queryInfo.queryStringTruncatedForPrint + '\'' +
                ", username='" + clientInfo.username + '\'' +
                ", client='" + clientInfo.client + '\'' +
                ", querySubmitTimestamp=" + querySubmitTimestamp +
                ", parsedStatement=" + parsedStatement +
                ", queryResourceCloser=" + queryResourceCloser +
                ", queryStartTimestamp=" + queryStartTimestamp +
                ", waitLock=" + waitLock +
                ", asynchronousRelease=" + asynchronousRelease +
                ", id=" + id +
                '}';
    }


}
