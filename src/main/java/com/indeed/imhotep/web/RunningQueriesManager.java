package com.indeed.imhotep.web;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

/**
 * @author vladimir
 */

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final IQLDB iqldb;

    private final List<SelectQuery> queriesWaiting = Lists.newArrayList();

    private final List<SelectQuery> queriesRunning = Lists.newArrayList();

    public List<RunningQuery> lastDaemonRunningQueries;

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
        final RunningQueriesUpdateResult result;
        try {
            synchronized (queriesWaiting) {
                if (queriesWaiting.size() == 0) {
                    return;
                }

                log.debug("Checking locks for " + queriesWaiting.size() + " pending queries");

                result = tryStartPendingQueries(queriesWaiting, iqldb);
                final List<SelectQuery> queriesStarted = result.queriesStarting;

                queriesWaiting.removeAll(result.queriesStarting);

                for(SelectQuery startedQuery: queriesStarted) {
                    startedQuery.onStarted(DateTime.now());
                }
            }

            synchronized (queriesRunning) {
                queriesRunning.addAll(result.queriesStarting);
                if(result.cancelledQueries.size() > 0) {
                    applyCancellations(result.cancelledQueries);
                }
            }
        } catch (Exception e) {
            log.error("Exception while starting waiting queries", e);
        }
    }

    private static class RunningQueriesUpdateResult {
        List<SelectQuery> queriesStarting;
        LongSet cancelledQueries;

        public RunningQueriesUpdateResult(List<SelectQuery> queriesStarting, LongSet cancelledQueries) {
            this.queriesStarting = queriesStarting;
            this.cancelledQueries = cancelledQueries;
        }
    }

    private static RunningQueriesUpdateResult tryStartPendingQueries(List<SelectQuery> pendingQueries, IQLDB iqldb) {
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
        final Object2IntOpenHashMap<String> usernameToSessionsCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToSessionsCount = new Object2IntOpenHashMap<>();
        for(final RunningQuery runningQuery: alreadyRunningQueries) {
            if(runningQuery.killed) {
                cancelledQueries.add(runningQuery.id);
            }

            final SelectQuery pendingQuery = idToPendingQuery.get(runningQuery.id);
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

                if((qhashesRunning.contains(pendingQuery.queryHash) ||
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
        for(SelectQuery startingQuery: queriesStarting) {
            iqldb.setRunningQueryStartTime(queryExecutionStartTime, startingQuery.id);
        }
        log.debug("Started " + queriesStarting.size() + ", still pending " + queriesThatCouldntStart.size());

        return new RunningQueriesUpdateResult(queriesStarting, cancelledQueries);
    }

    private void applyCancellations(LongSet cancelledQueries) {
        for(SelectQuery runningQuery : queriesRunning) {
            if(cancelledQueries.contains(runningQuery.id)) {
                runningQuery.cancelled = true;
            }
        }
    }


    public void register(SelectQuery selectQuery) {
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
                        + ". " + selectQuery.queryStringTruncatedForPrint, e);
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) { }
            }
        }
    }

    public boolean isEnabled() {
        return iqldb != null;
    }
}
