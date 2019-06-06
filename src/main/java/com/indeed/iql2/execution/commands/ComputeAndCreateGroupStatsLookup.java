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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ComputeAndCreateGroupStatsLookup implements Command {
    public final Command computation;
    public final String name;

    public ComputeAndCreateGroupStatsLookup(final Command computation, final String name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException, IOException {
        if (ComputeAndCreateGroupStatsLookups.tryMultiDistinct(session, Collections.singletonList(new Pair<>(computation, name)))) {
            return;
        }

        double[] results = null;
        long[] longResults = null;
        if (computation instanceof GetGroupDistincts) {
            longResults = ((GetGroupDistincts)computation).evaluate(session);
        } else if(computation instanceof GetGroupDistinctsWindowed) {
            longResults = ((GetGroupDistinctsWindowed)computation).evaluate(session);
        } else if(computation instanceof GetSimpleGroupDistincts) {
            longResults = ((GetSimpleGroupDistincts)computation).evaluate(session);
        } else if(computation instanceof SumAcross) {
            results = ((SumAcross)computation).evaluate(session);
        } else if(computation instanceof ComputeFieldExtremeValue) {
            results = ((ComputeFieldExtremeValue)computation).evaluate(session);
        } else if (computation instanceof GetGroupPercentiles) {
            longResults = ((GetGroupPercentiles)computation).evaluate(session);
        } else if (computation instanceof GetGroupStats) {
            final double[][] groupStats = ((GetGroupStats)computation).evaluate(session);
            results = Arrays.copyOf(groupStats[0], session.getNumGroups() + 1);
        } else {
            throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookup parser: " + computation);
        }
        if (longResults != null) {
            results = ComputeAndCreateGroupStatsLookups.longToDouble(longResults);
        }
        new CreateGroupStatsLookup(results, name).execute(session);
    }
}
