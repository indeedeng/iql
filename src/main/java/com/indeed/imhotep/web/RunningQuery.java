package com.indeed.imhotep.web;

import org.joda.time.DateTime;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

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
    public final DateTime executionStartTime;
    public final String hostname;
    public final byte sessions;
    public final boolean killed;

    public RunningQuery(long id, String query, String qHash, String username, String client, DateTime submitTime, DateTime executionStartTime, String hostname, byte sessions, boolean killed) {
        this.id = id;
        this.query = query;
        this.qHash = qHash;
        this.username = username;
        this.client = client;
        this.submitTime = submitTime;
        this.executionStartTime = executionStartTime;
        this.hostname = hostname;
        this.sessions = sessions;
        this.killed = killed;
    }

    public String getSubmitTime() {
        return submitTime.toString();
    }

    public String getExecutionStartTime() {
        return executionStartTime.toString();
    }

    @Override
    public String toString() {
        return "RunningQuery{\n" +
                "\n id=" + id +
                "\n qHash=" + qHash +
                "\n username=" + username +
                "\n client=" + client +
                "\n submitTime=" + submitTime +
                "\n executionStartTime=" + executionStartTime +
                "\n hostname=" + hostname +
                "\n sessions=" + sessions +
                "\n killed=" + killed +
                "\n query=" + query +
                "\n}";
    }

    public static RowMapper<RunningQuery> resultSetRowMapper = new RowMapper<RunningQuery>() {
        @Override
        public RunningQuery mapRow(ResultSet rs, int rowNum) throws SQLException {
            final Timestamp submitTime = rs.getTimestamp("submit_time");
            final Timestamp executionStartTime = rs.getTimestamp("execution_start_time");

            return new RunningQuery(
                    rs.getLong("id"),
                    rs.getString("query"),
                    rs.getString("qhash"),
                    rs.getString("username"),
                    rs.getString("client"),
                    submitTime != null ? new DateTime(submitTime.getTime()) : null,
                    executionStartTime != null ? new DateTime(executionStartTime.getTime()) : null,
                    rs.getString("hostname"),
                    rs.getByte("sessions"),
                    rs.getBoolean("killed"));
        }
    };
}
