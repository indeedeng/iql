/*
 * Copyright (C) 2014 Indeed Inc.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.io.Files;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metadata.DatasetMetadata;
import com.indeed.imhotep.metadata.FieldMetadata;
import com.indeed.imhotep.metadata.FieldType;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */

public class TopTermsCache {
    private static final Logger log = Logger.getLogger(TopTermsCache.class);
    private static final int TERMS_TO_CACHE = 100;
    private static final int DAYS_DELAY = 2;
    private static final String CACHE_FILE_NAME = "toptermscache.bin";
    private static final int CACHE_UPDATE_FREQUENCY = 24 * 60 * 60 * 1000; // 24 hours;
    private final ImhotepClient client;
    private final String localCachePath;
    private boolean initialized = false;
    private final boolean devMode;

    private volatile Map<String, Map<String, List<String>>> datasetToFieldToTerms = Maps.newHashMap();

    public TopTermsCache(ImhotepClient client, String localCachePath, boolean devMode) {
        this.client = client;
        this.localCachePath = localCachePath;
        this.devMode = devMode;
    }

    @Scheduled(fixedRate = CACHE_UPDATE_FREQUENCY)
    public void updateTopTerms() {
        final File cacheFile = new File(localCachePath, CACHE_FILE_NAME);
        final String cacheFilePath = cacheFile.getAbsolutePath();
        if((!initialized || devMode) && cacheFile.exists()) {
            try {
                final TopTermsArtifact artifact = Files.readObjectFromFile(cacheFilePath, TopTermsArtifact.class, true);

                if(artifact != null) {
                    final DateTime artifactExpirationTime = new DateTime(artifact.timestamp).plusMillis(CACHE_UPDATE_FREQUENCY);
                    if(DateTime.now().isBefore(artifactExpirationTime) || devMode) {
                        // persisted cache not expired. reuse
                        datasetToFieldToTerms = artifact.datasetToFieldToTerms;
                        initialized = true;
                        return;
                    }
                }
            } catch (Exception e) {
                log.warn("Exception while reading " + CACHE_FILE_NAME, e);
            }
        }

        // on further invocations, reload from imhotep
        log.info("Starting TopTerms cache update. This may take a few minutes");
        long started = System.currentTimeMillis();
        datasetToFieldToTerms = updateTopTermsFromImhotep();
        log.info("TopTerms cache update completed in " + (System.currentTimeMillis()-started)/1000 + "s");
        initialized = true;

        try {
            Files.writeObjectToFileOrDie(new TopTermsArtifact(System.currentTimeMillis(), datasetToFieldToTerms), cacheFilePath);
        } catch (IOException e) {
            log.warn("Failed to serialize top terms cache to " + cacheFilePath);
        }
    }

    private Map<String, Map<String, List<String>>> updateTopTermsFromImhotep() {
        final Map<String, Map<String, List<String>>> newDatasetToFieldToTerms = Maps.newHashMap();
        final DateTime startTime = DateTime.now().minusDays(DAYS_DELAY).withTimeAtStartOfDay().plusHours(12);
        final DateTime endTime = startTime.plusHours(1);

        for(final String dataset : client.getDatasetNames()) {
            long started = System.currentTimeMillis();

            final Map<String, List<String>> fieldToTerms = Maps.newHashMap();

            final ImhotepSession imhotepSession;
            try {
                final ImhotepClient.SessionBuilder sessionBuilder = client.sessionBuilder(dataset, startTime, endTime).username("IQL: topterms");
                if(sessionBuilder.getChosenShards().size() == 0) {
                    log.info("Index " + dataset + " has no shards for midday " + DAYS_DELAY + " days ago");
                    continue;
                }
                 imhotepSession = sessionBuilder.build();
            } catch (Exception e) {
                log.warn("Failed to create a session for " + dataset + " " + startTime + " - " + endTime);
                continue;
            }

            try {
                // we are trying to get some values for enum like string fields. can skip the random integer values
                final Collection<String> stringFields = client.getDatasetInfo(dataset).getStringFields();
                for(final String field : stringFields) {
                    final List<TermCount> termCounts = imhotepSession.approximateTopTerms(field, false, TERMS_TO_CACHE);

                    if(termCounts.size() == 0) {
                        log.debug(dataset + "." + field + " has no terms");
                    }

                    final List<String> terms = Lists.newArrayList();
                    for(TermCount termCount : termCounts) {
                        terms.add(termCount.getTerm().getTermStringVal());
                    }
                    fieldToTerms.put(field, terms);
                }
            } finally {
                Closeables2.closeQuietly(imhotepSession, log);
            }

            if(fieldToTerms.size() > 0) {
                newDatasetToFieldToTerms.put(dataset, fieldToTerms);
            }

            long tookSeconds = (System.currentTimeMillis()-started)/1000;
            if(tookSeconds > 1) {
                log.debug("TopTerms for " + dataset + " loaded in " + tookSeconds + "s");
            }
        }
        return newDatasetToFieldToTerms;
    }

    public List<String> getTopTerms(String dataset, String field) {
        final Map<String, List<String>> fieldToTerms = datasetToFieldToTerms.get(dataset);
        if(fieldToTerms == null) {
            return Collections.emptyList();
        }

        final List<String> terms = fieldToTerms.get(field);
        if(terms == null) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(terms);
    }
}
