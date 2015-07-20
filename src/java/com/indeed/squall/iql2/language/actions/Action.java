package com.indeed.squall.iql2.language.actions;

import com.indeed.squall.iql2.language.compat.Consumer;

import java.util.Map;
import java.util.Set;

public interface Action {
    void validate(Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields, Consumer<String> errorConsumer);
}
