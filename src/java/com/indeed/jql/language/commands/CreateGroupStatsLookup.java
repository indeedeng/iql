package com.indeed.jql.language.commands;

import com.google.common.base.Optional;

public class CreateGroupStatsLookup implements Command {
    public final double[] stats;
    public final Optional<String> name;

    public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
        this.stats = stats;
        this.name = name;
    }
}
