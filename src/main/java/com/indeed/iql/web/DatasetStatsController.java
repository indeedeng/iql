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

    private final DatasetStatsCache datasetStatsCache;

    @Autowired
    public DatasetStatsController(
            DatasetStatsCache datasetStatsCache
    ) {
        this.datasetStatsCache = datasetStatsCache;
    }

    @RequestMapping(value = "/datasetstats", produces = "application/json")
    @ResponseBody
    public List<DatasetStats> doGet() {
        datasetStatsCache.updateDatasetStatsCache();
        return datasetStatsCache.getDatasetStats();
    }

    @RequestMapping(value = "/typeconflictfields", produces = "application/json")
    @ResponseBody
    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        datasetStatsCache.updateDatasetStatsCache();
        return datasetStatsCache.getTypeConflictFields();
    }
}
