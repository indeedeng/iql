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
 package com.indeed.imhotep.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Keeps track of the currently running queries.
 * All public methods on this class operating on internal data structures should be marked synchronized.
 * @author vladimir
 */
@Component
public class ExecutionManager {
    private static final Logger log = Logger.getLogger(ExecutionManager.class);
    // values are used for locking to avoid concurrent processing of identical requests
    private final Map<String, CountDownLatch> queryToLock = Maps.newHashMap();
    // used to limit number of concurrent queries per user
    private final Map<String, Semaphore> userToLock = Maps.newHashMap();
    private final Set<QueryTracker> runningQueries = Sets.newHashSet();

    @Value("${user.concurrent.query.limit}")
    private int maxQueriesPerUser;

    public ExecutionManager() {
    }

    @Nonnull
    public synchronized List<QueryTracker> getRunningQueries() {
        return Lists.newArrayList(runningQueries);
    }

    /**
     * Keeps track of the query that is going to be executed and makes sure that user allocated limit is not exceeded.
     * When the query execution is completed (data is in HDFS cache) or fails, the returned Query object must be closed.
     */
    public synchronized QueryTracker queryStarted(String query, String username) throws TimeoutException {
        final CountDownLatch releaseLock;
        final CountDownLatch waitLockForQuery;
        final Semaphore waitLockForUser;
        if(queryToLock.containsKey(query)) {   // this is a duplicate query and execution will have to wait
            waitLockForQuery = queryToLock.get(query);
            waitLockForUser = null;
            releaseLock = null;
        } else {    // this is a non-duplicate query and the lock has to be released after execution is finished
            waitLockForQuery = null;
            waitLockForUser = getUserSemaphore(username);
            releaseLock = new CountDownLatch(1);
            queryToLock.put(query, releaseLock);
        }
        final QueryTracker newQueryTracker = new QueryTracker(username, query, waitLockForQuery, waitLockForUser, releaseLock, this);
        runningQueries.add(newQueryTracker);
        return newQueryTracker;
    }

    private synchronized Semaphore getUserSemaphore(String username) {
        Semaphore semaphore = userToLock.get(username);
        if(semaphore == null) {
            semaphore = new Semaphore(maxQueriesPerUser, true);
            userToLock.put(username, semaphore);
        }
        return semaphore;
    }

    private synchronized void release(QueryTracker q) {
        if(q.released) {
            return; // release called twice
        }
        q.released = true;

        if(q.releaseLock != null) { // this was the original query of this type and it's done now
            q.releaseLock.countDown();
            queryToLock.remove(q.query);
        }
        if(q.userSlotUsed && q.waitLockForUser != null) {
            q.waitLockForUser.release();
        }

        runningQueries.remove(q);
    }

    /**
     * Keeps track of the query execution.
     * Must be closed when all operations relating to the query processing are complete (including HDFS cache upload).
     */
    @JsonPropertyOrder({"startedTime", "username", "query"})
    public class QueryTracker implements Closeable {
        private final String username;  // user running the query
        private final String query; // query text
        private final CountDownLatch waitLockForQuery;  // lock to wait on before the query starts
        private final Semaphore waitLockForUser;  // lock to wait on before the query starts
        private final CountDownLatch releaseLock;   // lock to be released when the query is done
        private final ExecutionManager owner;
        private final DateTime startedTime = DateTime.now();
        private boolean asynchronousRelease = false;
        private boolean released = false;
        private boolean userSlotUsed = false;

        private QueryTracker(String username, String query, CountDownLatch waitLockForQuery, Semaphore waitLockForUser, CountDownLatch releaseLock, ExecutionManager owner) {
            this.username = username;
            this.query = query;
            this.waitLockForQuery = waitLockForQuery;
            this.waitLockForUser = waitLockForUser;
            this.releaseLock = releaseLock;
            this.owner = owner;
        }

        public String getUsername() {
            return username;
        }

        @JsonIgnore
        public String getQuery() {
            return query;
        }

        @JsonProperty("query")
        public String getQueryTruncated() {
            String queryTruncated = QueryServlet.shortenParamsInQuery(query);
            if (queryTruncated.length() > 500) {
                queryTruncated = queryTruncated.substring(0, 500);
            }
            return queryTruncated;
        }

        public String getStartedTime() {
            return startedTime.toString();
        }

        public void acquireLocks() throws TimeoutException {
            waitForQueryLock();
            waitForUserLock();
        }

        private void waitForQueryLock() throws TimeoutException {
            if(waitLockForQuery == null) {
                return;
            }
            // same query is already being handled, waiting
            try {
                if(!waitLockForQuery.await(5, TimeUnit.MINUTES)) {
                    log.error("Reached timeout waiting for completion of: " + query);
                    throw new TimeoutException("Reached timeout (5 min) waiting for completion of original execution of the query");
                }
            } catch (InterruptedException ignored) {
                throw new RuntimeException("Interrupted while waiting for completion of original execution of the query. You can retry.");
            }
        }

        private void waitForUserLock() throws TimeoutException {
            if(waitLockForUser == null) {
                return;
            }

            try {
                if(!waitLockForUser.tryAcquire(5, TimeUnit.MINUTES)) {
                    throw new TimeoutException("Reached timeout (5 min) waiting in queue for query execution");
                }
                userSlotUsed = true;
            } catch (InterruptedException ignored) {
                throw new RuntimeException("Wait in queue for query execution was interrupted. You can retry.");
            }
        }

        @Override
        public void close() throws IOException {
            owner.release(this);
        }

        public void markAsynchronousRelease() {
            this.asynchronousRelease = true;
        }

        @JsonIgnore
        public boolean isAsynchronousRelease() {
            return asynchronousRelease;
        }
    }
}
