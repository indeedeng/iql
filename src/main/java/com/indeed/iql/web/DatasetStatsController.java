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

import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * @author vladimir
 */

@Controller
public class DatasetStatsController {
    private static final Logger log = Logger.getLogger(DatasetStatsController.class);
    private static final Period CACHE_EXPIRATION = new Period(23, 0, 0, 0);

    private DateTime lastCacheUpdate = new DateTime(0);
    private List<DatasetStats> cached = null;

    private final ImhotepClient imhotepClient;

    private final ImhotepMetadataCache metadataCache;

    @Autowired
    public DatasetStatsController(
            final ImhotepClient imhotepClient,
            final ImhotepMetadataCache metadataCache
    ) {
        this.imhotepClient = imhotepClient;
        this.metadataCache = metadataCache;
    }

    @RequestMapping(value = "/datasetstats", produces = "application/json")
    @ResponseBody
    public List<DatasetStats> doGet() {
        updateDatasetStatsCache();

        return cached;
    }

    private synchronized void updateDatasetStatsCache() {
        if (cached == null || DateTime.now().isAfter(lastCacheUpdate.plus(CACHE_EXPIRATION))) {
            long computationStartTime = System.currentTimeMillis();
            cached = DatasetStatsCollector.computeStats(imhotepClient, metadataCache);
            lastCacheUpdate = DateTime.now();
            long timeTaken = System.currentTimeMillis() - computationStartTime;
            log.info("Computed Imhotep datasets stats in " + timeTaken + " ms. Cached for " + CACHE_EXPIRATION);
        }
    }

    @RequestMapping(value = "/typeconflictfields", produces = "application/json")
    @ResponseBody
    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        return metadataCache.get().getTypeConflictFields();
    }
}
