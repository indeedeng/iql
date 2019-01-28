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

package com.indeed.iql.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.indeed.imhotep.api.PerformanceStats;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryInfo {
    private static final Logger log = Logger.getLogger(QueryInfo.class);

    private static Pattern queryTruncatePattern = Pattern.compile("\\(([^\\)]{0,200}+)[^\\)]+\\)");
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public QueryInfo(String query, int iqlVersion, long queryStartTimestamp, @Nullable String sqlQuery) {
        this.queryLength = query.length();
        this.queryStringTruncatedForPrint = truncateQuery(query);
        this.sqlQuery = truncateQuery(sqlQuery);
        this.iqlVersion = iqlVersion;
        this.queryStartTimestamp = queryStartTimestamp;
    }

    @Nullable
    public static String truncateQuery(@Nullable final String query) {
        return query == null ? null : queryTruncatePattern.matcher(query).replaceAll("\\($1\\.\\.\\.\\)");
    }

    @JsonIgnore
    public @Nullable String statementType;
    @JsonIgnore
    public String queryStringTruncatedForPrint;
    @JsonProperty("query")
    public String queryStringTruncatedForPrint() {
        return queryStringTruncatedForPrint;
    }
    public @Nullable long queryStartTimestamp;
    public final int iqlVersion;
    public int queryLength;
    public @Nullable Set<String> datasets;
    public @Nullable Integer totalDatasetRangeDays; // SUM(dataset (End - Start)). duration in FROM even if missing shards
    public @Nullable Integer totalShardPeriodHours; // SUM(shard (end-start)). time actually covered by shards
    public @Nullable Long ftgsMB;
    public @Nullable Long imhotepcputimems;
    public @Nullable Long imhoteprammb;
    public @Nullable Long imhotepftgsmb;
    public @Nullable Long imhotepfieldfilesmb;
    public @Nullable Long cpuSlotsExecTimeMs;
    public @Nullable Long cpuSlotsWaitTimeMs;
    public @Nullable Long ioSlotsExecTimeMs;
    public @Nullable Long ioSlotsWaitTimeMs;
    public @Nullable Long queryId;
    public @Nullable Set<String> sessionIDs;
    public @Nullable Integer numShards;
    public @Nullable Long numDocs;
    public @Nullable Set<String> imhotepServers;
    public @Nullable Integer numImhotepServers;
    public @Nullable Boolean cached;
    public @Nullable Integer rows;
    public @Nullable Boolean cacheUploadSkipped;
    public @Nullable Long resultBytes;
    public @Nullable Set<String> cacheHashes;
    public @Nullable Integer maxGroups = 0;
    public @Nullable Integer maxConcurrentSessions;
    public @Nullable Set<String> datasetFields;
    public @Nullable Set<String> datasetFieldsNoDescription;
    public @Nullable Boolean fieldHadDescription;
    public @Nullable Integer selectCount;
    public @Nullable Integer groupByCount;
    public @Nullable Boolean headOnly;

    public @Nullable String timingTreeReport;
    public @Nullable Long totalTime;

    public @Nullable Long lockWaitMillis;
    public @Nullable Long cacheCheckMillis;
    public @Nullable Long sendToClientMillis;
    public @Nullable Long shardsSelectionMillis;
    public @Nullable Long createSessionMillis;
    public @Nullable Long timeFilterMillis;
    public @Nullable Long conditionFilterMillis;
    public @Nullable Long regroupMillis;
    public @Nullable Long ftgsMillis;
    public @Nullable Long pushStatsMillis;
    public @Nullable Long getStatsMillis;

    public @Nullable String sqlQuery;


    public void setFromPerformanceStats(PerformanceStats performanceStats) {
        if (performanceStats == null) {
            return;
        }
        imhotepcputimems = TimeUnit.NANOSECONDS.toMillis(performanceStats.cpuTime);
        imhoteprammb = performanceStats.maxMemoryUsage / 1024 / 1024;
        imhotepftgsmb = performanceStats.ftgsTempFileSize / 1024 / 1024;
        imhotepfieldfilesmb = performanceStats.fieldFilesReadSize / 1024 / 1024;
        cpuSlotsExecTimeMs = performanceStats.cpuSlotsExecTimeMs;
        cpuSlotsWaitTimeMs = performanceStats.cpuSlotsWaitTimeMs;
        ioSlotsExecTimeMs = performanceStats.ioSlotsExecTimeMs;
        ioSlotsWaitTimeMs = performanceStats.ioSlotsWaitTimeMs;
    }

    public String toJSON() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize query info as JSON", e);
            return "{}";
        }

    }
}
