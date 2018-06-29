/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.web;

import com.google.common.collect.Maps;
import com.indeed.squall.iql2.language.util.FieldExtractor;
import com.indeed.util.core.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

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

    public void insertRunningQuery(final SelectQuery query) {
        // Hack to workaround the column not allowing nulls
        final long queryExecutionStartTime = 1;

        final PreparedStatementCreator psc = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                final PreparedStatement ps = connection.prepareStatement("INSERT INTO tblrunning " +
                                "(query, qhash, username, client, submit_time, execution_start_time, hostname, sessions) " +
                                "VALUES (?, ?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, StringUtils.abbreviate(query.queryStringTruncatedForPrint, 1000));
                ps.setString(2, StringUtils.abbreviate(query.queryHash, 30));
                ps.setString(3, StringUtils.abbreviate(query.clientInfo.username, 100));
                ps.setString(4, StringUtils.abbreviate(query.clientInfo.client, 100));
                ps.setLong(5, query.querySubmitTimestamp.getMillis() / 1000);
                ps.setLong(6, queryExecutionStartTime);
                ps.setString(7, StringUtils.abbreviate(hostname, 20));
                ps.setByte(8, query.sessions);
                return ps;
            }
        };
        final KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(psc, keyHolder);
        query.onInserted(keyHolder.getKey().longValue());
    }

    public void setRunningQueryStartTime(long queryExecutionStartTimeSecondsSinceEpoch, long id) {
        jdbcTemplate.update("UPDATE tblrunning SET execution_start_time = FROM_UNIXTIME(?) WHERE id = ?",
                queryExecutionStartTimeSecondsSinceEpoch, id);
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
        String query = "SELECT id, query, qhash, username, client, UNIX_TIMESTAMP(submit_time) as submit_time, " +
                "UNIX_TIMESTAMP(execution_start_time) as execution_start_time, hostname, sessions, killed FROM tblrunning";
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

    private final static String SQL_STATEMENT_DELETE_OUTDATED_RECORDS = "DELETE FROM tblfieldfreq WHERE used_date < CURDATE() - INTERVAL 60 DAY";

    private final static String SQL_STATEMENT_SELECT_DATASET_FIELD_FREQUENCY = "SELECT dataset, field, SUM(count) AS frequency FROM tblfieldfreq GROUP BY dataset, field";

    private final static String SQL_STATEMENT_INCREMENT_FIELD_FREQUENCY = "INSERT INTO tblfieldfreq (`dataset`, `field`, `used_date`, `count`) VALUES (?, ?, CURDATE(), 1) ON DUPLICATE KEY UPDATE count = count + 1";

    private void deleteOutdatedFields() {
        jdbcTemplate.execute(SQL_STATEMENT_DELETE_OUTDATED_RECORDS);
    }

    public Map<String, Map<String, Integer>> getDatasetFieldFrequencies() {
        deleteOutdatedFields();

        final SqlRowSet srs = jdbcTemplate.queryForRowSet(SQL_STATEMENT_SELECT_DATASET_FIELD_FREQUENCY);
        final Map<String, Map<String, Integer>> datasetFieldFrequencies = Maps.newHashMap();
        while (srs.next()) {
            final String dataset = srs.getString("dataset");
            final Map<String, Integer> fieldToFrequency = datasetFieldFrequencies.computeIfAbsent(dataset, key -> Maps.newHashMap());
            final String field = srs.getString("field");
            final Integer frequency = srs.getInt("frequency");
            fieldToFrequency.put(field, frequency);
        }

        return datasetFieldFrequencies;
    }

    public void incrementFieldFrequencies(final List<String> datasetFields) {
        jdbcTemplate.batchUpdate(SQL_STATEMENT_INCREMENT_FIELD_FREQUENCY,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(final PreparedStatement preparedStatement, final int i) throws SQLException {
                        final String[] pair = datasetFields.get(i).split(Pattern.quote("."));
                        preparedStatement.setString(1, pair[0]);
                        preparedStatement.setString(2, pair[1]);
                    }
                    @Override
                    public int getBatchSize() {
                        return datasetFields.size();
                    }
                });
    }

}
