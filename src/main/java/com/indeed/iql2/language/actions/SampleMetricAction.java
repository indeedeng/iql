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

import com.google.common.collect.ImmutableMap;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class SampleMetricAction implements Action {
    final ImmutableMap<String, DocMetric> perDatasetMetric;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final ImmutableMap<String, DocMetric> perDatasetMetric, final double probability, final String seed, final int targetGroup, final int positiveGroup, final int negativeGroup) {
        this.perDatasetMetric = perDatasetMetric;
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction() {
        return new com.indeed.iql2.execution.actions.SampleMetricAction(
                perDatasetMetric,
                probability,
                seed,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }
}
