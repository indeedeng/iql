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

    private static final Pattern queryTruncatePattern = Pattern.compile("\\(([^\\)]{0,200}+)[^\\)]+\\)");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public QueryInfo(final String hostname, String query, int iqlVersion, long queryStartTimestamp, @Nullable String sqlQuery) {
        this.hostname = hostname;
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

    @Nullable
    @JsonIgnore
    public String statementType;
    @JsonIgnore
    public String queryStringTruncatedForPrint;
    @JsonProperty("query")
    public String queryStringTruncatedForPrint() {
        return queryStringTruncatedForPrint;
    }
    public long queryStartTimestamp;
    public final int iqlVersion;
    public String engine;
    public int queryLength;
    public final String hostname;
    @Nullable public Set<String> datasets;
    @Nullable public Integer totalDatasetRangeDays; // SUM(dataset (End - Start)). duration in FROM even if missing shards
    @Nullable public Integer totalShardPeriodHours; // SUM(shard (end-start)). time actually covered by shards
    @Nullable public Long ftgsMB;
    @Nullable public Long imhotepcputimems;
    @Nullable public Long imhoteprammb;
    @Nullable public Long imhotepftgsmb;
    @Nullable public Long imhotepfieldfilesmb;
    @Nullable public Long cpuSlotsExecTimeMs;
    @Nullable public Long cpuSlotsWaitTimeMs;
    @Nullable public Long ioSlotsExecTimeMs;
    @Nullable public Long ioSlotsWaitTimeMs;
    @Nullable public Long queryId;
    @Nullable public Set<String> sessionIDs;
    @Nullable public Integer numShards;
    @Nullable public Long numDocs;
    @Nullable public Set<String> imhotepServers;
    @Nullable public Integer numImhotepServers;
    @Nullable public Boolean cached;
    @Nullable public Integer rows;
    @Nullable public Boolean cacheUploadSkipped;
    @Nullable public Long resultBytes;
    @Nullable public Set<String> cacheHashes;
    @Nullable public Integer maxGroups = 0;
    @Nullable public Integer maxConcurrentSessions;
    @Nullable public Set<String> datasetFields;
    @Nullable public Set<String> datasetFieldsNoDescription;
    @Nullable public Boolean fieldHadDescription;
    @Nullable public Integer selectCount;
    @Nullable public Integer groupByCount;
    @Nullable public Boolean headOnly;
    @Nullable public Long priority;

    @Nullable public String timingTreeReport;
    @Nullable public Long totalTime;

    @Nullable public Long lockWaitMillis;
    @Nullable public Long cacheCheckMillis;
    @Nullable public Long sendToClientMillis;
    @Nullable public Long shardsSelectionMillis;
    @Nullable public Long createSessionMillis;
    @Nullable public Long timeFilterMillis;
    @Nullable public Long conditionFilterMillis;
    @Nullable public Long regroupMillis;
    @Nullable public Long ftgsMillis;
    @Nullable public Long pushStatsMillis;
    @Nullable public Long getStatsMillis;

    @Nullable public String sqlQuery;

    @Nullable public Long imhotepFilesDownloadedMB;
    @Nullable public Long imhotepP2PFilesDownloadedMB;

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
        final Long downloadedBytes = performanceStats.customStats.get("downloadedBytes");
        if (downloadedBytes != null) {
            imhotepFilesDownloadedMB = downloadedBytes / 1024 / 1024;
        }
        final Long downloadedBytesP2P = performanceStats.customStats.get("downloadedBytesP2P");
        if (downloadedBytesP2P != null) {
            imhotepP2PFilesDownloadedMB = downloadedBytesP2P / 1024 / 1024;
        }
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
