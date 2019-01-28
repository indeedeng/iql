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

package com.indeed.iql2.language.actions;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.Map;

public class SampleMetricAction implements Action {
    final ImmutableMap<String, DocMetric> perDatasetMetric;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final Map<String, DocMetric> perDatasetMetrics,
                              final double probability,
                              final String seed,
                              final int targetGroup,
                              final int positiveGroup,
                              final int negativeGroup) {
        this.perDatasetMetric = ImmutableMap.copyOf(perDatasetMetrics);
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.iql2.execution.actions.SampleMetricAction(
                perDatasetMetric,
                probability,
                seed,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }

    @Override
    public String toString() {
        return "SampleMetricAction{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
