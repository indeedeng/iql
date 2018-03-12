package com.indeed.imhotep.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.joda.time.DateTime;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * @author vladimir
 */
public class RunningQuery {
    public final long id;
    public final String query;
    public final String qHash;
    public final String username;
    public final String client;
    public final DateTime submitTime;
    public final DateTime startedTime;
    public final String hostname;
    @JsonIgnore
    public final byte sessions;
    @JsonIgnore
    public final boolean killed;

    public RunningQuery(long id, String query, String qHash, String username, String client, DateTime submitTime, DateTime startedTime, String hostname, byte sessions, boolean killed) {
        this.id = id;
        this.query = query;
        this.qHash = qHash;
        this.username = username;
        this.client = client;
        this.submitTime = submitTime;
        this.startedTime = startedTime;
        this.hostname = hostname;
        this.sessions = sessions;
        this.killed = killed;
    }

    public String getSubmitTime() {
        return submitTime.toString();
    }

    public String getStartedTime() {
        return startedTime != null ? startedTime.toString() : null;
    }

    @Override
    public String toString() {
        return "RunningQuery{\n" +
                "\n id=" + id +
                "\n qHash=" + qHash +
                "\n username=" + username +
                "\n client=" + client +
                "\n submitTime=" + submitTime +
                "\n startedTime=" + startedTime +
                "\n hostname=" + hostname +
                "\n sessions=" + sessions +
                "\n killed=" + killed +
                "\n query=" + query +
                "\n}";
    }

    public static RowMapper<RunningQuery> resultSetRowMapper = new RowMapper<RunningQuery>() {
        @Override
        public RunningQuery mapRow(ResultSet rs, int rowNum) throws SQLException {
            final long submitTime = rs.getLong("submit_time");
            long executionStartTime = rs.getLong("execution_start_time");
            if(executionStartTime < TimeUnit.DAYS.toSeconds(2)) {
                executionStartTime = 0;  // Hack to workaround the column not allowing nulls
            }

            return new RunningQuery(
                    rs.getLong("id"),
                    rs.getString("query"),
                    rs.getString("qhash"),
                    rs.getString("username"),
                    rs.getString("client"),
                    submitTime != 0 ? new DateTime(submitTime * 1000) : null,
                    executionStartTime != 0 ? new DateTime(executionStartTime * 1000) : null,
                    rs.getString("hostname"),
                    rs.getByte("sessions"),
                    rs.getBoolean("killed"));
        }
    };

    public static RowMapper<RunningQuery> resultSetRowMapperForLocking = new RowMapper<RunningQuery>() {
        @Override
        public RunningQuery mapRow(ResultSet rs, int rowNum) throws SQLException {

            return new RunningQuery(
                    rs.getLong("id"),
                    null,
                    rs.getString("qhash"),
                    rs.getString("username"),
                    rs.getString("client"),
                    null,
                    null,
                    null,
                    rs.getByte("sessions"),
                    rs.getBoolean("killed"));
        }
    };
}
