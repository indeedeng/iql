package com.indeed.imhotep.web;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author vladimir
 */

public class RunningQueriesManager {
    private static final Logger log = Logger.getLogger ( RunningQueriesManager.class );
    private final IQLDB iqldb;
    List<SelectQuery> queries = Lists.newArrayList();
    List<SelectQuery> waiting = Lists.newArrayList();

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
            // sorted in SQL now
//            Collections.sort(lastDaemonRunningQueries, new Comparator<RunningQuery>() {
//                @Override
//                public int compare(RunningQuery o1, RunningQuery o2) {
//                    return o1.executionStartTime.compareTo(o2.executionStartTime);
//                }
//            });
            log.warn("Daemon was in the process of running the following queries when it was shutdown: ");
            for (RunningQuery query : lastDaemonRunningQueries) {
                log.warn(query.toString());
            }

            iqldb.clearRunningForThisHost();
        }
    }


    public void register(SelectQuery selectQuery) {
        //waiting.add(selectQuery);
        boolean started = false;
        while(!started) {
            started = iqldb.tryStartRunningQuery(selectQuery);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {}
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
                        + ". " + selectQuery.queryStringTruncatedForPrint);
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
