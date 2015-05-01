package com.indeed.squall.jql.commands;

import com.indeed.squall.jql.Session;

import java.util.Optional;

public class CreateGroupStatsLookup {
    public final double[] stats;
    public final Optional<String> name;

    public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
        this.stats = stats;
        this.name = name;
    }

    public String execute(Session session) {
        final int depth = session.currentDepth;
        final double[] stats = this.stats;
        final Session.SavedGroupStats savedStats = new Session.SavedGroupStats(depth, stats);
        final String lookupName;
        if (this.name.isPresent()) {
            lookupName = this.name.get();
            if (session.savedGroupStats.containsKey(lookupName)) {
                throw new IllegalArgumentException("Name already in use!: [" + lookupName + "]");
            }
        } else {
            lookupName = String.valueOf(session.savedGroupStats.size());
        }
        session.savedGroupStats.put(lookupName, savedStats);
        return lookupName;
    }
}
