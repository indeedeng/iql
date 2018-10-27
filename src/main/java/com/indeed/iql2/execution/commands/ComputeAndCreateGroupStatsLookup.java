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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class ComputeAndCreateGroupStatsLookup implements Command {
    public final Command computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(final Command computation, final Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final String lookupName = evaluate(session);
        if (lookupName != null) {
            out.accept(Session.MAPPER.writeValueAsString(Arrays.asList(lookupName))); // from CreateGroupStatsLookup
        }
    }

    public String evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        if (name.isPresent()) {
            if (ComputeAndCreateGroupStatsLookups.tryMultiDistinct(session, Collections.singletonList(new Pair<>((Command) computation, name.get())))) {
                return null;
            }
        }

        double[] results = null;
        long[] longResults = null;
        if (computation instanceof GetGroupDistincts) {
            longResults = ((GetGroupDistincts)computation).evaluate(session);
        } else if(computation instanceof GetSimpleGroupDistincts) {
            longResults = ((GetSimpleGroupDistincts)computation).evaluate(session);
        } else if(computation instanceof SumAcross) {
            results = ((SumAcross)computation).evaluate(session);
        } else if(computation instanceof GetFieldMin) {
            longResults = ((GetFieldMin)computation).evaluate(session);
        } else if(computation instanceof GetFieldMax) {
            longResults = ((GetFieldMax)computation).evaluate(session);
        } else if (computation instanceof GetGroupPercentiles) {
            final long[][] percentiles = ((GetGroupPercentiles)computation).evaluate(session);
            Preconditions.checkState(percentiles.length == 1, "Only one percentile expected");
            longResults = percentiles[0];
        } else if (computation instanceof GetGroupStats) {
            final List<Session.GroupStats> groupStats = ((GetGroupStats)computation).evaluate(session);
            results = new double[groupStats.size()];
            for (int i = 0; i < groupStats.size(); i++) {
                results[i] = groupStats.get(i).stats[0];
            }
        } else if (computation instanceof ComputeBootstrap) {
            ((ComputeBootstrap)computation).execute(session);
            // This already did stuff internally
            return "ABSTRACT NONSENSE";
        } else {
            throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookup parser: " + computation);
        }
        if (longResults != null) {
            results = ComputeAndCreateGroupStatsLookups.longToDouble(longResults);
        }
        return new CreateGroupStatsLookup(Session.prependZero(results), this.name).execute(session);
    }
}
