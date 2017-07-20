package com.indeed.imhotep.web;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author vladimir
 */
public class RunningQuery {
    public final long id;
    public final String query;
    public final String qHash;
    public final String username;
    public final DateTime submitTime;
    public final DateTime executionStartTime;
    public final String hostname;
    public final boolean killed;

    public RunningQuery(long id, String query, String qHash, String username, DateTime submitTime, DateTime executionStartTime, String hostname, boolean killed) {
        this.id = id;
        this.query = query;
        this.qHash = qHash;
        this.username = username;
        this.submitTime = submitTime;
        this.executionStartTime = executionStartTime;
        this.hostname = hostname;
        this.killed = killed;
    }

    @Override
    public String toString() {
        return "RunningQuery{\n" +
                "\n id=" + id +
                "\n qHash=" + qHash +
                "\n username=" + username +
                "\n submitTime=" + submitTime +
                "\n executionStartTime=" + executionStartTime +
                "\n hostname=" + hostname +
                "\n killed=" + killed +
                "\n query=" + query +
                "\n}";
    }

    public static ResultSetExtractor<List<RunningQuery>> resultSetExtractor = new ResultSetExtractor<List<RunningQuery>>() {
        @Override
        public List<RunningQuery> extractData(ResultSet rs) throws SQLException, DataAccessException {
            List<RunningQuery> list = Lists.newArrayList();
            while (rs.next()) {
                final Timestamp submitTime = rs.getTimestamp("submit_time");
                final Timestamp executionStartTime = rs.getTimestamp("execuction_start_time");

                list.add(new RunningQuery(
                        rs.getLong("id"),
                        rs.getString("query"),
                        rs.getString("qhash"),
                        rs.getString("username"),
                        submitTime != null ? new DateTime(submitTime.getTime()) : null,
                        executionStartTime != null ? new DateTime(executionStartTime.getTime()) : null,
                        rs.getString("hostname"),
                        rs.getBoolean("killed")));
            }
            return list;
        }
    };
}
