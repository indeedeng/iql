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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.ImhotepClient;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

/**
 * @author vladimir
 */
public class DatasetStatsCollector {
    private static final Logger log = Logger.getLogger(DatasetStatsCollector.class);
    public static final DateTimeFormatter SHARD_VERSION_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZone(DateTimeZone.forOffsetHours(-6));
    public static final long LOWEST_LEGAL_TIMESTAMP_DATE = 10000000000L;


    public static List<DatasetStats> computeStats(ImhotepClient client) {
        final List<DatasetStats> statsList = Lists.newArrayList();
        Map<String, DatasetInfo> datasetToDatasetInfo = client.getDatasetToDatasetInfo();
        Map<String, Collection<ShardInfo>> datasetToShardList = client.queryDatasetToFullShardList();
        for (DatasetInfo datasetInfo : datasetToDatasetInfo.values()) {
            final DatasetStats stats = new DatasetStats();
            statsList.add(stats);
            stats.name = datasetInfo.getDataset();
            stats.numIntFields = datasetInfo.getIntFields().size();
            stats.numStrFields = datasetInfo.getStringFields().size();
            stats.typeConflictFields = Sets.intersection(new HashSet<>(datasetInfo.getIntFields()),
                    new HashSet<>(datasetInfo.getStringFields()));
            stats.numTypeConflictFields = stats.typeConflictFields.size();
            long lastShardTimestamp = 0;
            long firstShardTimestamp = Long.MAX_VALUE;
            long lastShardNumDocs = 0;
            long firstDataTime = Long.MAX_VALUE;
            long lastDataTime = 0;
            final IntOpenHashSet shardSizes = new IntOpenHashSet();
            final Collection<ShardInfo> shardList = datasetToShardList.get(datasetInfo.getDataset());
            for (ShardInfo shard : shardList) {
                stats.numDocs += shard.getNumDocs();
                stats.numShards++;
                if (shard.version > lastShardTimestamp) {
                    lastShardTimestamp = shard.version;
                }
                if(shard.version < firstShardTimestamp && shard.version != 0) {
                    firstShardTimestamp = shard.version;
                }
                final ShardInfo.DateTimeRange range = shard.getRange();
                if (range == null) {
                    log.info("Failed to parse time range for " + datasetInfo.getDataset() + ":" + shard.getShardId() +
                            "." + shard.getVersion());
                } else {
                    final int shardSizeHours = (int) new Duration(range.start, range.end).getStandardHours();
                    if (!shardSizes.contains(shardSizeHours)) {
                        shardSizes.add(shardSizeHours);
                        stats.shardSizesHours.add(shardSizeHours);
                    }
                    if(range.start.getMillis() < firstDataTime) {
                        firstDataTime = range.start.getMillis();
                    }
                    // TODO order by version
                    if(range.end.getMillis() > lastDataTime) {
                        lastDataTime = range.end.getMillis();
                        lastShardNumDocs = shard.numDocs;
                    }
                }
            }
            if (lastShardTimestamp > LOWEST_LEGAL_TIMESTAMP_DATE) {
                try {
                    stats.lastShardBuildTimestamp = SHARD_VERSION_FORMATTER.parseDateTime(String.valueOf(lastShardTimestamp));
                } catch (Exception e) {
                    log.warn("Failed to parse datetime from version " + lastShardTimestamp + " for dataset " + datasetInfo.getDataset());
                }
            }
            if (firstShardTimestamp != Long.MAX_VALUE && firstShardTimestamp > LOWEST_LEGAL_TIMESTAMP_DATE) {
                try {
                    stats.firstShardBuildTimestamp = SHARD_VERSION_FORMATTER.parseDateTime(String.valueOf(firstShardTimestamp));
                } catch (Exception e) {
                    log.warn("Failed to parse datetime from version " + firstShardTimestamp + " for dataset " + datasetInfo.getDataset());
                }
            }



            if (firstDataTime != Long.MAX_VALUE && firstDataTime != 0) {
                stats.firstDataTime = new DateTime(firstDataTime, DateTimeZone.forOffsetHours(-6));
            } else {
                log.error("Failed to parse time range start for " + datasetInfo.getDataset());
            }
            if (lastDataTime != 0) {
                stats.lastDataTime = new DateTime(lastDataTime, DateTimeZone.forOffsetHours(-6));
                stats.lastShardNumDocs = lastShardNumDocs;
                final long lastWeekCutoff = stats.lastDataTime.minusDays(7).getMillis();
                // reiterate through the shards to find the last week
                for(ShardInfo shard : shardList) {
                    final DateTime start = shard.getStart();
                    // TODO: don't recount the rebuilt shards
                    if (start != null && start.getMillis() >= lastWeekCutoff) {
                        stats.lastWeekNumDocs += shard.numDocs;
                    }
                }
            } else {
                log.error("Failed to parse time range end for " + datasetInfo.getDataset());
            }

            stats.reportGenerationTime = DateTime.now();
        }

        Collections.sort(statsList, new Comparator<DatasetStats>() {
            @Override
            public int compare(DatasetStats o1, DatasetStats o2) {
                return o1.name.compareTo(o2.name);
            }
        });

        return statsList;
    }
}
