package com.indeed.imhotep.web;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


/**
 * @author vladimir
 */

public class IQLDB {
    private static final Logger log = Logger.getLogger ( IQLDB.class );
    public static String hostname;
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));

        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch (java.net.UnknownHostException ex) {
            hostname = "(unknown)";
        }
    }

    private final JdbcTemplate jdbcTemplate;

    public IQLDB(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /** Returns true iff query was registered successfully. **/
    @Transactional
    public boolean tryStartRunningQuery(SelectQuery query, int maxQueriesPerUser) {
        final String username = query.username;
        final String queryHash = query.queryHash;
        final Map<String, Object> sqlResult = jdbcTemplate.queryForMap(
            "SELECT COUNT(username = ?) AS queriesRunningForUser, COUNT(qhash = ?) AS runningWithSameHash " +
                    "FROM tblrunning order by id FOR UPDATE", username, queryHash);
        final long queriesRunningForUser = (Long) sqlResult.get("queriesRunningForUser");
        final long runningWithSameHash = (Long) sqlResult.get("runningWithSameHash");

        if (queriesRunningForUser >= maxQueriesPerUser) {
            return false;
        }

        if(runningWithSameHash > 0) {
            return false;
        }

        final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());

        jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname) VALUES (?, ?, ?, ?, ?, ?)",
                query.queryStringTruncatedForPrint, query.queryHash, query.username, query.client, new Timestamp(query.querySubmitTimestamp.getMillis()), queryExecutionStartTime, hostname);
        try {
            Long id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
            query.onStarting(id, new DateTime(queryExecutionStartTime));
            query.onStarted();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return true;
    }

    @Transactional
    public List<SelectQuery> tryStartPendingQueries(List<SelectQuery> pendingQueries, int maxQueriesPerUser) {
        final List<RunningQuery> alreadyRunningQueries = getRunningQueries(null, true);
        final List<SelectQuery> queriesThatCouldntStart = Lists.newArrayList();
        final List<SelectQuery> queriesStarting = Lists.newArrayList();
        final Set<String> qhashesRunning = Sets.newHashSet();
        final Map<String, Integer> usernamesToRunningCount = Maps.newHashMap();
        for(RunningQuery runningQuery: alreadyRunningQueries) {
            qhashesRunning.add(runningQuery.qHash);
            final String username = runningQuery.username;
            Integer runningForUserSoFar = usernamesToRunningCount.get(username);
            if(runningForUserSoFar == null) {
                runningForUserSoFar = 0;
            }
            usernamesToRunningCount.put(username, runningForUserSoFar + 1);
        }
//        final List<Object[]> insertArgs = Lists.newArrayList();
        final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());
        for(SelectQuery pendingQuery: pendingQueries) {
            Integer queriesRunningForUser = usernamesToRunningCount.get(pendingQuery.username);
            if(queriesRunningForUser == null) {
                queriesRunningForUser = 0;
            }
            if(queriesRunningForUser >= maxQueriesPerUser || qhashesRunning.contains(pendingQuery.queryHash)) {
                queriesThatCouldntStart.add(pendingQuery);
            } else {
                queriesStarting.add(pendingQuery);
                usernamesToRunningCount.put(pendingQuery.username, queriesRunningForUser + 1);
                qhashesRunning.add(pendingQuery.queryHash);
            }
        }

        for(SelectQuery startingQuery: queriesStarting) {
            Object[] args = new Object[] {
                    startingQuery.queryStringTruncatedForPrint,
                    startingQuery.queryHash,
                    startingQuery.username,
                    startingQuery.client,
                    new Timestamp(startingQuery.querySubmitTimestamp.getMillis()),
                    queryExecutionStartTime,
                    hostname
            };

            jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)", args);
            Long id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
            startingQuery.onStarting(id, new DateTime(queryExecutionStartTime));

            // TODO: use batching implemented below if reliable
            // Observed behavior of last_insert_id() with batchUpdate contradicts the doc
            // https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
//            insertArgs.add(args);
        }

//        jdbcTemplate.batchUpdate("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname) " +
//                "VALUES (?, ?, ?, ?, ?, ?, ?)", insertArgs);

//        Long id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
//        for(int i = queriesStarting.size() - 1; i >=0; i--) {
//            queriesStarting.get(i).onStarting(id, new DateTime(queryExecutionStartTime));
//            queriesStarting.get(i).onStarted();
//            id--;
//        }
        log.debug("Started " + queriesStarting.size() + ", still pending " + queriesThatCouldntStart.size());

        return queriesStarting;
    }

    public void deleteRunningQuery(long id) {
        int rowsDeleted = jdbcTemplate.update("DELETE FROM tblrunning WHERE id = ?", id);
        if(rowsDeleted != 1) {
            log.error("Couldn't find the row in tblrunning for query id " + id);
        }
    }

    public void clearRunningForThisHost() {
        jdbcTemplate.update("DELETE FROM tblrunning WHERE hostname = ?", hostname);
    }

    public List<RunningQuery> getRunningQueries() {
        return getRunningQueries(null);
    }

    public List<RunningQuery> getRunningQueriesForThisHost() {
        return getRunningQueries(hostname);
    }

    private List<RunningQuery> getRunningQueries(@Nullable String hostname) {
        return getRunningQueries(hostname, false);
    }

    private List<RunningQuery> getRunningQueries(@Nullable String hostname, boolean selectForUpdate) {
        String query = "SELECT id, query, qhash, username, client, submit_time, execution_start_time, hostname, killed FROM tblrunning";
        String[] args = new String[0];
        if(hostname != null) {
            query += " WHERE hostname = ?";
            args = new String[] {hostname};
        }
        query += " ORDER BY hostname, execution_start_time";
        if(selectForUpdate) {
            query += " FOR UPDATE";
        }
        return jdbcTemplate.query(query, RunningQuery.resultSetRowMapper, (Object[])args);
    }


}
