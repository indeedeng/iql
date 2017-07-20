package com.indeed.imhotep.web;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;
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

    @Value("${user.concurrent.query.limit}")
    private int maxQueriesPerUser;
    public IQLDB(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /** Returns true iff query was registered successfully. **/
    public boolean tryStartRunningQuery(SelectQuery query) {
        final String username = query.username;
        final String queryHash = query.queryHash;
        jdbcTemplate.execute("LOCK TABLES tblrunning WRITE");
        try {
            final long queriesRunningForUser = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM tblrunning WHERE username = ?", Long.class, username);
            if (queriesRunningForUser >= maxQueriesPerUser) {
                return false;
            }
            final long runningWithSameHash = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM tblrunning WHERE qhash = ?", Long.class, queryHash);
            if(runningWithSameHash > 0) {
                return false;
            }

            final Timestamp queryExecutionStartTime = new Timestamp(System.currentTimeMillis());

            jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, submit_time, execution_start_time, hostname) VALUES (?, ?, ?, ?, ?, ?)",
                    query.queryStringTruncatedForPrint, query.queryHash, query.username, new Timestamp(query.querySubmitTimestamp.getMillis()), queryExecutionStartTime, hostname);
            Long id;
            try {
                id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
                query.onStarted(id, new DateTime(queryExecutionStartTime));
            } catch (Exception e) {
                // TODO: we have to make sure this works
                Throwables.propagate(e);
            }
            return true;
        } finally {
            // TODO: we have to make sure this works
            jdbcTemplate.execute("UNLOCK TABLES");
        }
    }

    public void deleteRunningQuery(long id) {
        jdbcTemplate.update("DELETE FROM tblrunning WHERE id = ?", id);
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
        String query = "SELECT id, query, qhash, username, submit_time, execution_start_time, hostname, killed FROM tblrunning";
        String[] args = new String[0];
        if(hostname != null) {
            query += " WHERE hostname = ?";
            args = new String[] {hostname};
        }
        query += " ORDER BY hostname, execution_start_time";
        return jdbcTemplate.query(query, RunningQuery.resultSetRowMapper, (Object[])args);
    }


}
