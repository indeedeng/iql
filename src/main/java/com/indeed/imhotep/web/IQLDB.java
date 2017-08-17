package com.indeed.imhotep.web;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
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

    public void insertRunningQuery(SelectQuery query) {
        // Hack to workaround the column not allowing nulls
        final String queryExecutionStartTime = "1970-01-01 00:00:01";

        jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, client, submit_time, execution_start_time, hostname, sessions) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                StringUtils.abbreviate(query.queryStringTruncatedForPrint, 1000),
                StringUtils.abbreviate(query.queryHash, 30),
                StringUtils.abbreviate(query.clientInfo.username, 100),
                StringUtils.abbreviate(query.clientInfo.client, 100),
                new Timestamp(query.querySubmitTimestamp.getMillis()),
                queryExecutionStartTime,
                StringUtils.abbreviate(hostname, 20),
                query.sessions);


        // now that the insert succeeded we must make sure this succeeds so that we can delete the row when cleaning up
        while(true) {
            try {
                Long id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
                query.onInserted(id);
                break;
            } catch (Exception e) {
                log.error("Failed to get the inserted DB query id for " + query.shortHash +
                        ". Going to retry.", e);
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) { }
            }
        }
    }

    public void setRunningQueryStartTime(Timestamp queryExecutionStartTime, long id) {
        jdbcTemplate.update("UPDATE tblrunning SET execution_start_time = ? WHERE id = ?",
                queryExecutionStartTime, id);
    }

    public void cancelQuery(long id) {
        jdbcTemplate.update("UPDATE tblrunning SET killed = ? WHERE id = ?", 1, id);
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
        String query = "SELECT id, query, qhash, username, client, submit_time, execution_start_time, " +
                "hostname, sessions, killed FROM tblrunning";
        String[] args = new String[0];
        if(hostname != null) {
            query += " WHERE hostname = ?";
            args = new String[] {hostname};
        }
        query += " ORDER BY hostname, submit_time";
        return jdbcTemplate.query(query, RunningQuery.resultSetRowMapper, (Object[])args);
    }

    public List<RunningQuery> getRunningQueriesForLocking() {
        String query = "SELECT id, qhash, username, client, sessions, killed FROM tblrunning ORDER BY id";
        return jdbcTemplate.query(query, RunningQuery.resultSetRowMapperForLocking);
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
