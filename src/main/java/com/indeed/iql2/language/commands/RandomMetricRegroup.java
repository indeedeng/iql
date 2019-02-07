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

package com.indeed.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode
@ToString
public class RandomMetricRegroup implements Command {
    private final ImmutableMap<String, DocMetric> perDatasetMetric;
    private final int k;
    private final String salt;

    public RandomMetricRegroup(final ImmutableMap<String, DocMetric> perDatasetMetric, final int k, final String salt) {
        this.perDatasetMetric = perDatasetMetric;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        if (k <= 1) {
            errorCollector.error("Bucket count in RANDOM() must be greater than 1, buckets = " + k);
        }
        for (final Map.Entry<String, DocMetric> entry : perDatasetMetric.entrySet()) {
            entry.getValue().validate(entry.getKey(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        final Map<String, List<String>> perDatasetCommands = perDatasetMetric.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableList.copyOf(entry.getValue().getPushes(entry.getKey()))
                ));
        return new com.indeed.iql2.execution.commands.RandomMetricRegroup(
                perDatasetCommands,
                k,
                salt
        );
    }
}
