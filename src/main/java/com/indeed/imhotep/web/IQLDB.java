package com.indeed.imhotep.web;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
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
    public boolean tryStartRunningQuery(SelectQuery query, int maxQueriesPerUser, int maxImhotepSessionsPerUser) {
        final String username = query.clientInfo.username;
        final String queryHash = query.queryHash;
        final Map<String, Object> sqlResult = jdbcTemplate.queryForMap(
            "SELECT COUNT(username = ?) AS queriesRunningForUser, COUNT(qhash = ?) AS runningWithSameHash, " +
                    "COUNT(sessions = ?) AS sessionsForUser " +
                    "FROM tblrunning order by id FOR UPDATE", username, queryHash);
        final long queriesRunningForUser = (Long) sqlResult.get("queriesRunningForUser");
        final long sessionsForUser = (Long) sqlResult.get("sessionsForUser");
        final long runningWithSameHash = (Long) sqlResult.get("runningWithSameHash");

        if (queriesRunningForUser >= maxQueriesPerUser) {
            return false;
        }

        if (sessionsForUser >= maxImhotepSessionsPerUser) {
            return false;
        }

        if(runningWithSameHash > 0) {
            return false;
        }

        final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());

        jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname) VALUES (?, ?, ?, ?, ?, ?, ?)",
                query.queryStringTruncatedForPrint.substring(0, 1000),
                query.queryHash.substring(0, 30),
                query.clientInfo.username.substring(0, 100),
                query.clientInfo.client.substring(0, 100),
                new Timestamp(query.querySubmitTimestamp.getMillis()),
                queryExecutionStartTime,
                hostname.substring(0, 20),
                query.sessions);
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
    public List<SelectQuery> tryStartPendingQueries(List<SelectQuery> pendingQueries) {
        final List<RunningQuery> alreadyRunningQueries = getRunningQueries(null, true);
        final List<SelectQuery> queriesThatCouldntStart = Lists.newArrayList();
        final List<SelectQuery> queriesStarting = Lists.newArrayList();
        final Set<String> qhashesRunning = Sets.newHashSet();
        final Object2IntOpenHashMap<String> usernameToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> usernameToSessionsCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToRunningCount = new Object2IntOpenHashMap<>();
        final Object2IntOpenHashMap<String> clientToSessionsCount = new Object2IntOpenHashMap<>();
        for(RunningQuery runningQuery: alreadyRunningQueries) {
            qhashesRunning.add(runningQuery.qHash);
            final String username = runningQuery.username;
            final String client = runningQuery.client;
            usernameToRunningCount.add(username, 1);
            clientToRunningCount.add(client, 1);
            usernameToSessionsCount.add(username, runningQuery.sessions);
            clientToSessionsCount.add(client, runningQuery.sessions);
        }
//        final List<Object[]> insertArgs = Lists.newArrayList();
        final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());
        for(SelectQuery pendingQuery: pendingQueries) {
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

            if(qhashesRunning.contains(pendingQuery.queryHash) ||
                    !pendingQuery.limits.satisfiesConcurrentQueriesLimit(queriesRunningForIdentity) ||
                    !pendingQuery.limits.satisfiesConcurrentImhotepSessionsLimit(sessionsForIdentity)) {
                queriesThatCouldntStart.add(pendingQuery);
            } else {
                queriesStarting.add(pendingQuery);
                qhashesRunning.add(pendingQuery.queryHash);

                usernameToRunningCount.add(username, 1);
                clientToRunningCount.add(client, 1);
                usernameToSessionsCount.add(username, pendingQuery.sessions);
                clientToSessionsCount.add(client, pendingQuery.sessions);
            }
        }

        for(SelectQuery startingQuery: queriesStarting) {
            Object[] args = new Object[] {
                    startingQuery.queryStringTruncatedForPrint.substring(0, 1000),
                    startingQuery.queryHash.substring(0, 30),
                    startingQuery.clientInfo.username.substring(0, 100),
                    startingQuery.clientInfo.client.substring(0, 100),
                    new Timestamp(startingQuery.querySubmitTimestamp.getMillis()),
                    queryExecutionStartTime,
                    hostname.substring(0, 20),
                    startingQuery.sessions
            };

            jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname, sessions) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", args);
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

    public int clearRunningForThisHost() {
        return clearRunningForHost(hostname);
    }

    public int clearRunningForHost(@Nonnull String hostname) {
        return jdbcTemplate.update("DELETE FROM tblrunning WHERE hostname = ?", hostname);
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
        String query = "SELECT id, query, qhash, username, client, submit_time, execution_start_time, hostname, sessions, killed FROM tblrunning";
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

    public Map<String, Limits> getAccessLimits() {
         final List<Map<String, Object>> tbllimits = jdbcTemplate.queryForList("SELECT id, name, parent_id, " +
                 "query_document_count_limit_billions, query_in_memory_rows_limit, " +
                "query_ftgs_iql_limit_mb, query_ftgs_imhotep_daemon_limit_mb, concurrent_queries_limit, " +
                "concurrent_imhotep_sessions_limit FROM tbllimits");

         final Map<Integer, LimitsWithParent> idToLimits = Maps.newHashMap();
         for(Map<String, Object> row: tbllimits) {
             final int id = (int)row.get("id");
             final String name = (String)row.get("name");
             final Integer parentId = (Integer) row.get("parent_id");
             final Limits limits = new Limits(
                     (Integer) row.get("query_document_count_limit_billions"),
                     (Integer) row.get("query_in_memory_rows_limit"),
                     (Integer) row.get("query_ftgs_iql_limit_mb"),
                     (Integer) row.get("query_ftgs_imhotep_daemon_limit_mb"),
                     (Integer) row.get("concurrent_queries_limit"),
                     (Integer) row.get("concurrent_imhotep_sessions_limit")
                 );
             idToLimits.put(id, new LimitsWithParent(parentId, name, limits));
         }

         final Map<String, Limits> result = Maps.newHashMap();

         for(LimitsWithParent limitsWithParent: idToLimits.values()) {
             LimitsWithParent resolved = resolveLimitsParents(limitsWithParent, idToLimits, 0);
             result.put(resolved.name, resolved.limits);
         }
         return result;
    }

    private LimitsWithParent resolveLimitsParents(LimitsWithParent objectToResolve, Map<Integer, LimitsWithParent> idToLimits, int recursionDepth) {
        if(objectToResolve.parentId == null) {
            return objectToResolve;
        }

        if(recursionDepth > 10) {
            log.error("Found a probable loop in tbllimits references for parent_id " + objectToResolve.parentId);
            return objectToResolve;
        }
        final LimitsWithParent parent = idToLimits.get(objectToResolve.parentId);
        if(parent == null) {
            log.error("Found invalid parent_id in tbllimits: " + objectToResolve.parentId);
            return objectToResolve;
        }
        final Limits limits = objectToResolve.limits;
        final Limits parentLimits = parent.limits;

        final LimitsWithParent mergedWithParent = new LimitsWithParent(
                parent.parentId,
                objectToResolve.name,
                new Limits(
                        nullToDefault(limits.queryDocumentCountLimitBillions, parentLimits.queryDocumentCountLimitBillions),
                        nullToDefault(limits.queryInMemoryRowsLimit, parentLimits.queryInMemoryRowsLimit),
                        nullToDefault(limits.queryFTGSIQLLimitMB, parentLimits.queryFTGSIQLLimitMB),
                        nullToDefault(limits.queryFTGSImhotepDaemonLimitMB, parentLimits.queryFTGSImhotepDaemonLimitMB),
                        nullToDefault(limits.concurrentQueriesLimit, parentLimits.concurrentQueriesLimit),
                        nullToDefault(limits.concurrentImhotepSessionsLimit, parentLimits.concurrentImhotepSessionsLimit)
                )
        );

        return resolveLimitsParents(mergedWithParent, idToLimits, recursionDepth + 1);
    }

    private static Integer nullToDefault(Integer value, Integer defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static class LimitsWithParent {
        final Integer parentId;
        final String name;
        final Limits limits;

        public LimitsWithParent(Integer parentId, String name, Limits limits) {
            this.parentId = parentId;
            this.name = name;
            this.limits = limits;
        }
    }
}
