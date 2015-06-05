package com.indeed.jql.language.commands;

import com.indeed.common.util.Pair;

import java.util.List;

public class ComputeAndCreateGroupStatsLookups implements Command {
    private final List<Pair<Object, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Object, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }
}
