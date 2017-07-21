package com.indeed.imhotep.web;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;

import java.util.List;

/**
 * @author vladimir
 */

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final IQLDB iqldb;

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


    public void register(SelectQuery selectQuery) {
        boolean started = false;
        while(!started) {
            try {
                started = iqldb.tryStartRunningQuery(selectQuery);
            } catch (DataAccessException e) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {}
                continue;
            }

            if(!started) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    public void release(SelectQuery selectQuery) {
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
}
