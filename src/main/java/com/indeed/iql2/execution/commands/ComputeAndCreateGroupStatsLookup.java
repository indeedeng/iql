/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;;

import java.io.IOException;
import java.util.Collections;
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
        ((Command) computation).execute(session, new Consumer<String>() {
            @Override
            public void accept(String s) {
                reference.set(s);
            }
        });
        double[] results;
        if ((computation instanceof GetGroupDistincts)
                || (computation instanceof GetSimpleGroupDistincts)
                || (computation instanceof SumAcross)
                || (computation instanceof GetFieldMin)
                || (computation instanceof GetFieldMax)) {
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
        } else if (computation instanceof ComputeBootstrap) {
            // This already did stuff internally
            out.accept(Session.MAPPER.writeValueAsString(Collections.singletonList("ABSTRACT NONSENSE")));
            return;
        } else {
            throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookup parser: " + computation);
        }
        new CreateGroupStatsLookup(Session.prependZero(results), this.name).execute(session, out);
    }
}
