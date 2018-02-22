package com.indeed.imhotep.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Set;

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

    @Autowired
    public DatasetStatsController(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    @RequestMapping(value = "/datasetstats", produces = "application/json")
    @ResponseBody
    public List<DatasetStats> doGet() {
        updateDatasetStatsCache();

        return cached;
    }

    private synchronized void updateDatasetStatsCache() {
        if(cached == null || DateTime.now().isAfter(lastCacheUpdate.plus(CACHE_EXPIRATION))) {
            long computationStartTime = System.currentTimeMillis();
            cached = DatasetStatsCollector.computeStats(imhotepClient);
            lastCacheUpdate = DateTime.now();
            long timeTaken = System.currentTimeMillis() - computationStartTime;
            log.info("Computed Imhotep datasets stats in " + timeTaken + " ms. Cached for " + CACHE_EXPIRATION);
        }
    }

    @RequestMapping(value = "/typeconflictfields", produces = "application/json")
    @ResponseBody
    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        updateDatasetStatsCache();
        final List<DatasetTypeConflictFields> result = Lists.newArrayList();
        for(DatasetStats stats : cached) {
            if(stats.numTypeConflictFields > 0) {
                result.add(new DatasetTypeConflictFields(stats.name, stats.typeConflictFields));
            }
        }
        return result;
    }

    private static class DatasetTypeConflictFields {
        public String dataset;
        public Set<String> fields;

        public DatasetTypeConflictFields(String dataset, Set<String> fields) {
            this.dataset = dataset;
            this.fields = fields;
        }
    }
}
