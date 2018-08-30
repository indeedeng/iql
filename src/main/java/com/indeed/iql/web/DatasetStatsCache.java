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
import com.indeed.imhotep.client.ImhotepClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author vladimir
 */

public class DatasetStatsCache {
    private static final Logger log = Logger.getLogger(DatasetStatsCache.class);

    private static final long CACHE_UPDATE_FREQUENCY_MILLIS = 30 * 60 * 1000; // 30 minutes
    private static final Period CACHE_EXPIRATION = new Period(23, 0, 0, 0);

    private DateTime lastCacheUpdate = new DateTime(0);

    private volatile List<DatasetStats> cached = null;
    private volatile Set<String> cachedTypeConflictFieldNames = null;

    private final ImhotepClient imhotepClient;

    public DatasetStatsCache(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    public List<DatasetStats> getDatasetStats() {
        if (cached == null) {
            return Lists.newArrayList();
        }
        return cached;
    }

    /**
     *
     * @return A set of conflict field names, where each item is of the form "datasetname.fieldname"
     */
    public Set<String> getTypeConflictFieldNames() {
        if (cachedTypeConflictFieldNames == null) {
            return Sets.newHashSet();
        }
        return cachedTypeConflictFieldNames;
    }

    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        final List<DatasetStats> datasetStats = getDatasetStats();
        final List<DatasetTypeConflictFields> result = Lists.newArrayList();
        for(DatasetStats stats : datasetStats) {
            if(stats.numTypeConflictFields > 0) {
                result.add(new DatasetTypeConflictFields(stats.name, stats.typeConflictFields));
            }
        }
        return result;
    }

    private static Set<String> calculateTypeConflictFieldNames(List<DatasetStats> datasetStats) {
        final Set<String> typeConflictFieldNames = new HashSet<>();
        for (DatasetStats stats : datasetStats) {
            if (stats.numTypeConflictFields > 0) {
                for (String typeConflictField : stats.typeConflictFields) {
                    typeConflictFieldNames.add(stats.name + "." + typeConflictField);
                }
            }
        }
        return typeConflictFieldNames;
    }

    @Scheduled(fixedRate = CACHE_UPDATE_FREQUENCY_MILLIS)
    public synchronized void updateDatasetStatsCache() {
        if(cached == null || DateTime.now().isAfter(lastCacheUpdate.plus(CACHE_EXPIRATION))) {
            long computationStartTime = System.currentTimeMillis();
            cached = DatasetStatsCollector.computeStats(imhotepClient);
            cachedTypeConflictFieldNames = calculateTypeConflictFieldNames(cached);
            lastCacheUpdate = DateTime.now();
            long timeTaken = System.currentTimeMillis() - computationStartTime;
            log.info("Computed Imhotep datasets stats in " + timeTaken + " ms. Cached for " + CACHE_EXPIRATION);
        }
    }
}
