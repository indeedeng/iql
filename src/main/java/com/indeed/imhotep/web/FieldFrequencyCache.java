package com.indeed.imhotep.web;

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
			if(allowedClients == null || allowedClients.size() == 0 ||
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
