package com.indeed.squall.iql2.language.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtil {
    public static <T> Map<String, T> replicate(Map<String, String> scope, T query) {
        final Map<String, T> datasetToT = new HashMap<>();
        for (final String dataset : scope.keySet()) {
            datasetToT.put(dataset, query);
        }
        return datasetToT;
    }
}
