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

package com.indeed.iql2.execution.actions;

import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.language.DocMetric;
import com.indeed.util.logging.TracingTreeTimer;

import java.util.List;
import java.util.Map;

public class SampleMetricAction implements Action {
    public final ImmutableMap<String, DocMetric> perDatasetMetric;
    public final long numerator;
    public final long denominator;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final Map<String, DocMetric> perDatasetMetric,
                              final long numerator,
                              final long denominator,
                              final String seed,
                              final int targetGroup,
                              final int positiveGroup,
                              final int negativeGroup) {
        this.perDatasetMetric = ImmutableMap.copyOf(perDatasetMetric);
        this.numerator = numerator;
        this.denominator = denominator;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        session.process(new SessionCallback() {
            @Override
            public void handle(final TracingTreeTimer timer, final String name, final ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                if (!perDatasetMetric.containsKey(name)) {
                    return;
                }

                final List<String> stat = perDatasetMetric.get(name).getPushes(name);

                timer.push("randomMetricRegroup");
                final double probability = ((double)numerator) / denominator;
                session.randomMetricRegroup(stat, seed, 1.0 - probability, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        });
    }

    @Override
    public String toString() {
        return "SampleMetricAction{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", numerator=" + numerator +
                ", denominator=" + denominator +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
