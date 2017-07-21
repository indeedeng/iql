package com.indeed.imhotep.web;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

/**
 * @author vladimir
 */

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final IQLDB iqldb;

    private final List<SelectQuery> queriesWaiting = Lists.newArrayList();

    public List<RunningQuery> lastDaemonRunningQueries;

    @Value("${user.concurrent.query.limit}")
    private int maxQueriesPerUser;
    @Value("${user.concurrent.imhotep.sessions.limit}")
    private byte maxSessionsPerUser;


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
                if (queriesWaiting.size() > 0) {
                    log.debug("Checking locks for " + queriesWaiting.size() + " pending queries");

                    final List<SelectQuery> queriesStarted =
                            iqldb.tryStartPendingQueries(queriesWaiting, maxQueriesPerUser, maxSessionsPerUser);

                    for(SelectQuery startedQuery: queriesStarted) {
                        startedQuery.onStarted();
                    }
                    queriesWaiting.removeAll(queriesStarted);
                }
            }
        } catch (Exception e) {
            log.error("Exception while starting waiting queries", e);
        }
    }


    public void register(SelectQuery selectQuery) {
        synchronized (queriesWaiting) {
            queriesWaiting.add(selectQuery);
        }
//        boolean started = false;
//        while(!started) {
//            try {
//                started = iqldb.tryStartRunningQuery(selectQuery);
//            } catch (DataAccessException e) {
//                try {
//                    Thread.sleep(50);
//                } catch (InterruptedException ignored) {}
//                continue;
//            }
//
//            if(!started) {
//                try {
//                    Thread.sleep(500);
//                } catch (InterruptedException ignored) {}
//            }
//        }
    }

    public void unregister(SelectQuery selectQuery) {
        log.debug("Stopping to wait for locking of " + selectQuery.shortHash);
        synchronized (queriesWaiting) {
            queriesWaiting.remove(selectQuery);
        }
    }

    public void release(SelectQuery selectQuery) {
        log.debug("Deleting query from DB " + selectQuery.shortHash + " " + selectQuery.id);
        if(selectQuery.id <= 0) {
            throw new IllegalArgumentException("Tried to release a query that hasn't been locked. Id " + selectQuery.id
                    + ". " + selectQuery.queryStringTruncatedForPrint);
        }
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

    public List<SelectQuery> getQueriesWaiting() {
        return queriesWaiting;
    }
}
