package com.indeed.squall.iql2.execution.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ComputeAndCreateGroupStatsLookup implements Command {
    public final Object computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Object computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        // TODO: Seriously? Serializing to JSON and then back? To the same program?
        final AtomicReference<String> reference = new AtomicReference<>();
        final Object computation = this.computation;
        System.out.println("computation = " + computation);
        ((Command) computation).execute(session, new Consumer<String>() {
            @Override
            public void accept(String s) {
                reference.set(s);
            }
        });
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
        new CreateGroupStatsLookup(Session.prependZero(results), this.name).execute(session, out);
    }
}
