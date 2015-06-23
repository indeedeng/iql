package com.indeed.squall.jql.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ComputeAndCreateGroupStatsLookup {
    public final Object computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Object computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        // TODO: Seriously? Serializing to JSON and then back? To the same program?
        final AtomicReference<String> reference = new AtomicReference<>();
        final Object computation = this.computation;
        System.out.println("computation = " + computation);
        session.evaluateCommandInternal(null, new Consumer<String>() {
            @Override
            public void accept(String s) {
                reference.set(s);
            }
        }, computation);
        double[] results;
        if (computation instanceof GetGroupDistincts || computation instanceof SumAcross) {
            results = Session.MAPPER.readValue(reference.get(), new TypeReference<double[]>(){});
        } else if (computation instanceof GetGroupPercentiles) {
            final List<double[]> intellijDoesntLikeInlining = Session.MAPPER.readValue(reference.get(), new TypeReference<List<double[]>>(){});
            results = intellijDoesntLikeInlining.get(0);
        } else if (computation instanceof GetGroupStats) {
            final List<Session.GroupStats> groupStats = Session.MAPPER.readValue(reference.get(), new TypeReference<List<Session.GroupStats>>() {
            });
            results = new double[groupStats.size()];
            for (int i = 0; i < groupStats.size(); i++) {
                results[i] = groupStats.get(i).stats[0];
            }
        } else {
            throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookup parser.");
        }
        session.evaluateCommandInternal(null, out, new CreateGroupStatsLookup(Session.prependZero(results), this.name));
    }
}
