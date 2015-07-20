package com.indeed.squall.iql2.language.commands;

import com.google.common.base.Function;
import com.indeed.squall.iql2.language.compat.Consumer;

import java.util.Map;
import java.util.Set;

public interface Command {
    // TODO: Clean up this API
    void validate(
            Map<String, Set<String>> datasetToIntFields,
            Map<String, Set<String>> datasetToStringFields,
            Consumer<String> errorConsumer
    );
}
