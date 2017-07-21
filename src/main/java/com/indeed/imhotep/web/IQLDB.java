package com.indeed.imhotep.web;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

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

    @Value("${user.concurrent.query.limit}")
    private int maxQueriesPerUser;

    public IQLDB(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /** Returns true iff query was registered successfully. **/
    @Transactional
    public boolean tryStartRunningQuery(SelectQuery query) {
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

        jdbcTemplate.update("INSERT INTO tblrunning (query, qhash, username, submit_time, execution_start_time, hostname) VALUES (?, ?, ?, ?, ?, ?)",
                query.queryStringTruncatedForPrint, query.queryHash, query.username, new Timestamp(query.querySubmitTimestamp.getMillis()), queryExecutionStartTime, hostname);
        try {
            Long id = jdbcTemplate.queryForObject("SELECT last_insert_id()", Long.class);
            query.onStarted(id, new DateTime(queryExecutionStartTime));
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return true;
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
