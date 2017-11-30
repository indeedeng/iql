package com.indeed.squall.iql2.server.web.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.server.web.servlets.query.SelectQueryExecution;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final IQLDB iqldb;

    private final List<SelectQueryExecution> queriesWaiting = Lists.newArrayList();
    private final List<SelectQueryExecution> queriesRunning = Lists.newArrayList();

    public List<RunningQuery> lastDaemonRunningQueries;

    private volatile long lastTriedStartingQueries = 0;

    public RunningQueriesManager(IQLDB iqldb) {
        this.iqldb = iqldb;
    }

    public void onStartup() {
        if(!isEnabled()) {
            return;
        }
        lastDaemonRunningQueries = iqldb.getRunningQueriesForThisHost();
        if(lastDaemonRunningQueries.size() > 0) {
            log.warn("Daemon was in the process of running the following queries when it was shutdown: ");
            for (RunningQuery query : lastDaemonRunningQueries) {
                log.warn(query.toString());
            }

            iqldb.clearRunningForThisHost();
        }
    }

    @Scheduled(fixedDelay = 300)
    private void tryStartingWaitingQueries() {
        try {
            synchronized (queriesWaiting) {
                if (queriesWaiting.size() == 0) {
                    return;
                }
                // if there is a large volume of queries, batch them instead of letting each one trigger a DB check immediately
                if(lastTriedStartingQueries > System.currentTimeMillis() - 300) {
                    return;
                }
                lastTriedStartingQueries = System.currentTimeMillis();

                log.debug("Checking locks for " + queriesWaiting.size() + " pending queries");

                final RunningQueriesUpdateResult result = tryStartPendingQueries(queriesWaiting, iqldb);
                final List<SelectQueryExecution> queriesStarted = result.queriesStarting;

                queriesWaiting.removeAll(result.queriesStarting);

                synchronized (queriesRunning) {
                    queriesRunning.addAll(result.queriesStarting);
                    if(result.cancelledQueries.size() > 0) {
                        applyCancellations(result.cancelledQueries);
                    }
                }

                for(SelectQueryExecution startedQuery: queriesStarted) {
                    startedQuery.onStarted(DateTime.now().getMillis());
                }
            }
        } catch (Exception e) {
            log.error("Exception while starting waiting queries", e);
        }
    }

    private static class RunningQueriesUpdateResult {
        List<SelectQueryExecution> queriesStarting;
        LongSet cancelledQueries;

        public RunningQueriesUpdateResult(List<SelectQueryExecution> queriesStarting, LongSet cancelledQueries) {
            this.queriesStarting = queriesStarting;
            this.cancelledQueries = cancelledQueries;
        }
    }

    private static RunningQueriesUpdateResult tryStartPendingQueries(List<SelectQueryExecution> pendingQueries, IQLDB iqldb) {
        final LongSet cancelledQueries = new LongOpenHashSet();
        final Long2ObjectMap<SelectQueryExecution> idToPendingQuery = new Long2ObjectOpenHashMap<>();
        for(SelectQueryExecution query : pendingQueries) {
            if(query.id <= 0) {
                continue;   // it wasn't inserted yet
            }
            idToPendingQuery.put(query.id, query);
        }
        final List<RunningQuery> alreadyRunningQueries = iqldb.getRunningQueriesForLocking();
        final List<SelectQueryExecution> queriesThatCouldntStart = Lists.newArrayList();
        final List<SelectQueryExecution> queriesStarting = Lists.newArrayList();
        final Set<String> qhashesRunning = Sets.newHashSet();
        final Object2IntOpenHashMap<String> usernameToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> usernameToSessionsCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToSessionsCount = new Object2IntOpenHashMap<>();
        for(final RunningQuery runningQuery: alreadyRunningQueries) {
            if(runningQuery.killed) {
                cancelledQueries.add(runningQuery.id);
            }

            final SelectQueryExecution pendingQuery = idToPendingQuery.get(runningQuery.id);
            if(pendingQuery != null) {
                // the query we are looking at is something we may be able to start if the limits allow. let's check
                final int queriesRunningForIdentity;
                final int sessionsForIdentity;
                final String username = pendingQuery.clientInfo.username;
                final String client = pendingQuery.clientInfo.client;
                if(pendingQuery.clientInfo.isMultiuserClient) {
                    queriesRunningForIdentity = usernameToRunningCount.getInt(username);
                    sessionsForIdentity = usernameToSessionsCount.getInt(username);
                } else {
                    queriesRunningForIdentity = clientToRunningCount.getInt(client);
                    sessionsForIdentity = clientToSessionsCount.getInt(client);
                }

                if(runningQuery.killed) {
                    pendingQuery.cancelled = true;
                }

                if((qhashesRunning.contains(pendingQuery.query) ||
                        !pendingQuery.limits.satisfiesConcurrentQueriesLimit(queriesRunningForIdentity) ||
                        !pendingQuery.limits.satisfiesConcurrentImhotepSessionsLimit(sessionsForIdentity))
                        && !runningQuery.killed) {
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

        final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());
        for(SelectQueryExecution startingQuery: queriesStarting) {
            iqldb.setRunningQueryStartTime(queryExecutionStartTime, startingQuery.id);
        }
        log.debug("Started " + queriesStarting.size() + ", still pending " + queriesThatCouldntStart.size());

        return new RunningQueriesUpdateResult(queriesStarting, cancelledQueries);
    }

    private void applyCancellations(LongSet cancelledQueries) {
        for(SelectQueryExecution runningQuery : queriesRunning) {
            if(cancelledQueries.contains(runningQuery.id)) {
                runningQuery.cancelled = true;
            }
        }
    }


    public void register(SelectQueryExecution selectQuery) {
        iqldb.insertRunningQuery(selectQuery);
        synchronized (queriesWaiting) {
            queriesWaiting.add(selectQuery);
        }
        // Try to start the query immediately not waiting for the scheduled run of the DB updater
        tryStartingWaitingQueries();
    }

    public void unregister(SelectQueryExecution selectQuery) {
        synchronized (queriesWaiting) {
            if(queriesWaiting.remove(selectQuery)) {
                log.debug("Stopping to wait for locking of " + selectQuery.query);
            }
        }

        synchronized (queriesRunning) {
            queriesRunning.remove(selectQuery);
        }

        if(selectQuery.id <= 0) {
            return; // wasn't inserted in the DB yet
        }

        log.debug("Deleting query from DB " + selectQuery.query + " " + selectQuery.id);
        // it's important that we release the query so try until we do
        boolean released = false;
        while(!released) {
            try {
                iqldb.deleteRunningQuery(selectQuery.id);
                released = true;
            } catch (Exception e) {
                log.error("Failed to release query, going to retry. Id " + selectQuery.id
                        + ". " + selectQuery.query, e);
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

    private List<RunningQuery> convertSelectQueriesToRunningQueries(List<SelectQueryExecution> queries) {
        final List<RunningQuery> runningQueries = Lists.newArrayList();
        for(SelectQueryExecution query : queries) {
            runningQueries.add(new RunningQuery(
                    query.id,
                    query.query,
                    null,
                    query.clientInfo.username,
                    query.clientInfo.client,
                    null,
                    new DateTime(query.queryStartTimestamp),
                    IQLDB.hostname,
                    (byte) 1,
                    query.cancelled
            ));
        }
        return runningQueries;
    }
}

