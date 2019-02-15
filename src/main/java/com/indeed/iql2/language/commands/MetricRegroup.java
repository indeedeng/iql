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

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode
@ToString
public class MetricRegroup implements Command {
    public final ImmutableMap<String, DocMetric> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;
    public final boolean withDefault;
    public final boolean fromPredicate;

    public MetricRegroup(final ImmutableMap<String, DocMetric> perDatasetMetric, final long min, final long max, final long interval, final boolean excludeGutters, final boolean withDefault, final boolean fromPredicate) {
        this.perDatasetMetric = perDatasetMetric;
        this.min = min;
        this.max = max;
        this.interval = interval;
        this.excludeGutters = excludeGutters;
        this.withDefault = withDefault;
        this.fromPredicate = fromPredicate;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        if (interval <= 0) {
            errorCollector.error("Bucket size must be positive. size = " + interval);
        }
        if (min >= max) {
            errorCollector.error("Inverval minimum must be lower than interval maximum. Min = " + min + ", Max = " + max);
        }
        if ((max-min)%interval != 0) {
            final long bucketRange = max - min;
            errorCollector.error(MessageFormat.format("Bucket range should be a multiple of the interval. To correct, decrease the upper bound to {0} or increase to {1}", max - bucketRange % interval, max + interval - bucketRange % interval));
        }

        for (final Map.Entry<String, DocMetric> docMetricEntry : perDatasetMetric.entrySet()) {
            docMetricEntry.getValue().validate(docMetricEntry.getKey(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        final Map<String, List<String>> perDatasetCommands = perDatasetMetric.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableList.copyOf(entry.getValue().getPushes(entry.getKey()))
                ));
        return new com.indeed.iql2.execution.commands.MetricRegroup(
                perDatasetCommands,
                min,
                max,
                interval,
                excludeGutters,
                withDefault,
                fromPredicate
        );
    }
}
