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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.exceptions.QueryCancelledException;
import com.indeed.iql.exceptions.IqlKnownException;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Set;

/**
 * @author vladimir
 */

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final int perUserPendingQueriesLimit;
    private final IQLDB iqldb;

    private final List<SelectQuery> queriesWaiting = Lists.newArrayList();

    private final List<SelectQuery> queriesRunning = Lists.newArrayList();

    public List<RunningQuery> lastDaemonRunningQueries;

    private volatile long lastTriedStartingQueries = 0;

    public RunningQueriesManager(IQLDB iqldb, final int perUserPendingQueriesLimit) {
        this.iqldb = iqldb;
        this.perUserPendingQueriesLimit = perUserPendingQueriesLimit;
    }

    public void onStartup() {
        if(!isEnabled()) {
            return;
        }
        lastDaemonRunningQueries = iqldb.getRunningQueriesForThisHost();
        if(!lastDaemonRunningQueries.isEmpty()) {
            log.warn("Daemon was in the process of running the following queries when it was shutdown: ");
            for (RunningQuery query : lastDaemonRunningQueries) {
                log.warn(query.toString());
            }

            iqldb.clearRunningForThisHost();
        }
    }

    private static final int POLL_INTERVAL_MILLIS = 100;

    @Scheduled(fixedDelay = POLL_INTERVAL_MILLIS)
    private void tryStartingWaitingQueries() {
        try {
            synchronized (queriesWaiting) {
                if (queriesWaiting.isEmpty() && queriesRunning.isEmpty()) {
                    return;
                }
                // if there is a large volume of queries, batch them instead of letting each one trigger a DB check immediately
                if(lastTriedStartingQueries > System.currentTimeMillis() - POLL_INTERVAL_MILLIS) {
                    return;
                }
                lastTriedStartingQueries = System.currentTimeMillis();

                log.debug("Checking locks for " + queriesWaiting.size() + " pending queries");

                final RunningQueriesUpdateResult result = tryStartPendingQueries(queriesWaiting, iqldb, perUserPendingQueriesLimit);
                final List<SelectQuery> queriesStarted = result.queriesStarting;

                queriesWaiting.removeAll(result.queriesStarting);

                synchronized (queriesRunning) {
                    queriesRunning.addAll(result.queriesStarting);
                    if(!result.cancelledQueries.isEmpty()) {
                        applyCancellations(result.cancelledQueries);
                    }
                }

                for(SelectQuery startedQuery: queriesStarted) {
                    startedQuery.onStarted(DateTime.now());
                }
            }
        } catch (Exception e) {
            log.error("Exception while starting waiting queries", e);
        }
    }

    private static class RunningQueriesUpdateResult {
        final List<SelectQuery> queriesStarting;
        final LongSet cancelledQueries;

        RunningQueriesUpdateResult(List<SelectQuery> queriesStarting, LongSet cancelledQueries) {
            this.queriesStarting = queriesStarting;
            this.cancelledQueries = cancelledQueries;
        }
    }

    private static RunningQueriesUpdateResult tryStartPendingQueries(List<SelectQuery> pendingQueries, IQLDB iqldb, int perUserPendingQueriesLimit) {
        final LongSet cancelledQueries = new LongOpenHashSet();
        final Long2ObjectMap<SelectQuery> idToPendingQuery = new Long2ObjectOpenHashMap<>();
        for(SelectQuery query : pendingQueries) {
            if(query.id <= 0) {
                continue;   // it wasn't inserted yet
            }
            idToPendingQuery.put(query.id, query);
        }
        final List<RunningQuery> alreadyRunningQueries = iqldb.getRunningQueriesForLocking();
        final List<SelectQuery> queriesThatCouldntStart = Lists.newArrayList();
        final List<SelectQuery> queriesStarting = Lists.newArrayList();
        final Set<String> qhashesRunning = Sets.newHashSet();
        final Object2IntOpenHashMap<String> usernameToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> usernameToPendingCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> usernameToSessionsCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToPendingCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToSessionsCount = new Object2IntOpenHashMap<>();
        for(final RunningQuery runningQuery: alreadyRunningQueries) {
            if(runningQuery.killed) {
                cancelledQueries.add(runningQuery.id);
            }

            final SelectQuery pendingQuery = idToPendingQuery.get(runningQuery.id);
            if(pendingQuery != null) {
                // the query we are looking at is something we may be able to start if the limits allow. let's check
                final int queriesRunningForIdentity;
                final int queriesPendingForIdentity;
                final int sessionsForIdentity;
                final String username = pendingQuery.clientInfo.username;
                final String client = pendingQuery.clientInfo.client;
                if(pendingQuery.clientInfo.isMultiuserClient) {
                    queriesRunningForIdentity = usernameToRunningCount.getInt(username);
                    queriesPendingForIdentity = usernameToPendingCount.getInt(username);
                    sessionsForIdentity = usernameToSessionsCount.getInt(username);
                } else {
                    queriesRunningForIdentity = clientToRunningCount.getInt(client);
                    queriesPendingForIdentity = clientToPendingCount.getInt(client);
                    sessionsForIdentity = clientToSessionsCount.getInt(client);
                }
                
                usernameToPendingCount.add(username, 1);
                clientToPendingCount.add(client, 1);

                if(runningQuery.killed) {
                    log.debug("Cancelling query " + runningQuery.qHash + " before it starts running");
                    pendingQuery.cancellationException = new QueryCancelledException("The query was cancelled during execution");
                }

                if (queriesPendingForIdentity > perUserPendingQueriesLimit) {
                    log.debug("Aborting query " + runningQuery.qHash + " before it starts running due to too many pending queries for identity.");
                    pendingQuery.cancellationException = new IqlKnownException.TooManyPendingQueriesException(
                            "Number of currently pending queries for " +
                            (pendingQuery.clientInfo.isMultiuserClient ? "user \"" + username : "client \"" + client) + "\"" +
                            " is too high(" + queriesPendingForIdentity + " >= " + perUserPendingQueriesLimit + "). " +
                            "Please wait for earlier queries to complete before submitting new queries."
                    );
                }

                if (pendingQuery.cancellationException != null) {
                    cancelledQueries.add(pendingQuery.id);
                }

                if((qhashesRunning.contains(pendingQuery.queryHash) ||
                        !pendingQuery.limits.satisfiesConcurrentQueriesLimit(queriesRunningForIdentity + 1) ||
                        !pendingQuery.limits.satisfiesConcurrentImhotepSessionsLimit(sessionsForIdentity + pendingQuery.sessions))
                        && !runningQuery.killed
                        && pendingQuery.cancellationException == null) {
                    queriesThatCouldntStart.add(pendingQuery);
                } else {
                    queriesStarting.add(pendingQuery);
                }
            }

            if(qhashesRunning.add(runningQuery.qHash)) {
                final String username = runningQuery.username;
                final String client = runningQuery.client;
                usernameToRunningCount.add(username, 1);
                clientToRunningCount.add(client, 1);
                usernameToSessionsCount.add(username, runningQuery.sessions);
                clientToSessionsCount.add(client, runningQuery.sessions);
            }
        }

        final long queryExecutionStartTimeSecondsSinceEpoch = System.currentTimeMillis() / 1000;
        for(SelectQuery startingQuery: queriesStarting) {
            iqldb.setRunningQueryStartTime(queryExecutionStartTimeSecondsSinceEpoch, startingQuery.id);
        }
        log.debug("Started " + queriesStarting.size() + ", still pending " + queriesThatCouldntStart.size());

        return new RunningQueriesUpdateResult(queriesStarting, cancelledQueries);
    }

    private void applyCancellations(LongSet cancelledQueries) {
        for(SelectQuery runningQuery : queriesRunning) {
            if(cancelledQueries.contains(runningQuery.id)) {
                if (runningQuery.cancellationException == null) {
                    runningQuery.cancellationException = new QueryCancelledException("The query was cancelled during execution");
                }
                runningQuery.kill();
            }
        }
    }


    public void register(SelectQuery selectQuery) {
        if (!isEnabled()) {
            //For unit test, data source is null, fall into this logic
            //no db rea/write, no query management
            selectQuery.onInserted(-1L);
            selectQuery.onStarted(DateTime.now());
            return;
        }

        iqldb.insertRunningQuery(selectQuery);
        synchronized (queriesWaiting) {
            queriesWaiting.add(selectQuery);
        }
    }

    public void unregister(SelectQuery selectQuery) {
        synchronized (queriesWaiting) {
            if(queriesWaiting.remove(selectQuery)) {
                log.debug("Stopping to wait for locking of " + selectQuery.shortHash);
            }
        }

        synchronized (queriesRunning) {
            queriesRunning.remove(selectQuery);
        }

        if(selectQuery.id <= 0) {
            return; // wasn't inserted in the DB yet
        }

        log.debug("Deleting query from DB " + selectQuery.shortHash + " " + selectQuery.id);
        // it's important that we release the query so try until we do
        boolean released = false;
        while(!released) {
            try {
                iqldb.deleteRunningQuery(selectQuery.id);
                released = true;
            } catch (Exception e) {
                log.error("Failed to release query, going to retry. Id " + selectQuery.id
                        + ". " + selectQuery.queryInfo.queryStringTruncatedForPrint, e);
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) { }
            }
        }
    }

    public boolean isEnabled() {
        return iqldb != null;
    }

    public List<RunningQuery> getRunningReportForThisProcess() {
        synchronized (queriesRunning) {
            return convertSelectQueriesToRunningQueries(queriesRunning);
        }
    }

    public List<RunningQuery> getWaitingReportForThisProcess() {
        synchronized (queriesWaiting) {
            return convertSelectQueriesToRunningQueries(queriesWaiting);
        }
    }

    private List<RunningQuery> convertSelectQueriesToRunningQueries(List<SelectQuery> queries) {
        final List<RunningQuery> runningQueries = Lists.newArrayList();
        for(SelectQuery query : queries) {
            runningQueries.add(new RunningQuery(
                    query.id,
                    query.queryInfo.queryStringTruncatedForPrint,
                    query.queryHash,
                    query.clientInfo.username,
                    query.clientInfo.client,
                    query.querySubmitTimestamp,
                    query.queryStartTimestamp,
                    IQLDB.hostname,
                    query.sessions,
                    query.cancellationException != null
            ));
        }
        return runningQueries;
    }
}
