package com.indeed.imhotep.web;

import com.indeed.imhotep.client.ImhotepClient;
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

    @Autowired
    public DatasetStatsController(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    @RequestMapping(value = "/datasetstats", produces = "application/json")
    @ResponseBody
    public List<DatasetStats> doGet() {
        if(cached == null || DateTime.now().isAfter(lastCacheUpdate.plus(CACHE_EXPIRATION))) {
            long computationStartTime = System.currentTimeMillis();
            cached = DatasetStatsCollector.computeStats(imhotepClient);
            lastCacheUpdate = DateTime.now();
            long timeTaken = System.currentTimeMillis() - computationStartTime;
            log.info("Computed Imhotep datasets stats in " + timeTaken + " ms. Cached for " + CACHE_EXPIRATION);
        }

        return cached;
    }
}
