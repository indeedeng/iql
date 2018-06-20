package com.indeed.imhotep.web;

import com.google.common.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jessec
 */

public class FieldFrequencyCache {

	private static final int CACHE_UDPATE_FREQUENCY = 24 * 60 * 60 * 1000;

	private AtomicReference<Map<String, Map<String, Integer>>> atomDatasetToFieldFrequencies = new AtomicReference(Maps.newHashMap());

	private final IQLDB iqldb;

	public FieldFrequencyCache(@Nullable final IQLDB iqldb) {
		this.iqldb = iqldb;
	}

	@Scheduled(fixedRate = CACHE_UDPATE_FREQUENCY)
	public void updateFieldFrequencies() {
		if (iqldb != null) {
			atomDatasetToFieldFrequencies.set(iqldb.getDatasetFieldFrequencies());
		}
	}

	public Map<String, Map<String, Integer>> getFieldFrequencies() {
		return atomDatasetToFieldFrequencies.get();
	}
}
