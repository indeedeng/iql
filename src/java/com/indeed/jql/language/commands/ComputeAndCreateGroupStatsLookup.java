package com.indeed.jql.language.commands;

import com.google.common.base.Optional;

public class ComputeAndCreateGroupStatsLookup implements Command {
    public final Command computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Command computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookup{" +
                "computation=" + computation +
                ", name=" + name +
                '}';
    }
}
