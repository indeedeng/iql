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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class FieldFrequencyCache {

	private static final int DATASET_FIELD_FREQUENCIES_UPDATE_RATE = 24 * 60 * 60 * 1000;

	private static final int MYSQL_BATCH_UPDATE_RATE = 3 * 60 * 1000;

	private final AtomicReference<Map<String, Map<String, Integer>>> atomDatasetToFieldFrequencies = new AtomicReference<>(Maps.newHashMap());

	private final List<String> datasetFieldToBatchUpdate = Collections.synchronizedList(Lists.newArrayList());

	@Nullable
	private final IQLDB iqldb;
	@Nullable
	private final Set<String> allowedClients;

    public FieldFrequencyCache(@Nullable final IQLDB iqldb) {
        this(iqldb, null);
    }

	public FieldFrequencyCache(@Nullable final IQLDB iqldb, @Nullable Set<String> allowedClients) {
		this.iqldb = iqldb;
		this.allowedClients = allowedClients;
	}

	@Scheduled(fixedRate = DATASET_FIELD_FREQUENCIES_UPDATE_RATE)
	public void updateFieldFrequencies() {
		if (iqldb != null) {
			atomDatasetToFieldFrequencies.set(iqldb.getDatasetFieldFrequencies());
		}
	}

	public Map<String, Map<String, Integer>> getFieldFrequencies() {
		return atomDatasetToFieldFrequencies.get();
	}

	public void acceptDatasetFields(final Set<String> newDatasetFields, ClientInfo clientInfo) {
		if (iqldb != null) {
			if(allowedClients == null || allowedClients.isEmpty() ||
					allowedClients.contains(clientInfo.client)) {
				datasetFieldToBatchUpdate.addAll(newDatasetFields);
			}
		}
	}

	@Scheduled(fixedRate = MYSQL_BATCH_UPDATE_RATE)
	public void batchUpdateDatasetFieldFrequencies() {
		if (iqldb != null) {
			iqldb.incrementFieldFrequencies(ImmutableList.copyOf(datasetFieldToBatchUpdate));
			datasetFieldToBatchUpdate.clear();
		}
	}
}
