package com.indeed.squall.iql2.execution.commands;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.util.Arrays;

public class CreateGroupStatsLookup implements Command {
    public final double[] stats;
    public final Optional<String> name;

    public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
        this.stats = stats;
        this.name = name;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws JsonProcessingException {
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
        out.accept(Session.MAPPER.writeValueAsString(Arrays.asList(lookupName)));
    }
}
