package com.indeed.squall.jql.commands;

import java.util.Optional;

public class ComputeAndCreateGroupStatsLookup {
    public final Object computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Object computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }
}
