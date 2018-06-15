package com.indeed.imhotep.web;

import com.google.common.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;

/**
 * @author jessec
 */

public class FieldFrequencyCache {

	private static final int CACHE_UDPATE_FREQUENCY = 24 * 60 * 60 * 1000;

	private Map<String, Map<String, Integer>> fieldFrequencies = Maps.newHashMap();

	private final IQLDB iqldb;

	public FieldFrequencyCache(final IQLDB iqldb) {
		this.iqldb = iqldb;
	}

	@Scheduled(fixedRate = CACHE_UDPATE_FREQUENCY)
	public void updateFieldFrequencies() {
		fieldFrequencies = iqldb.getDatasetFieldFrequencies();
	}

	public Map<String, Map<String, Integer>> getFieldFrequencies() {
		return fieldFrequencies;
	}
}
