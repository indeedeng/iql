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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.SortOrder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.GroupBy;

@EqualsAndHashCode
@ToString
public class TopK {
    public final Optional<Long> limit;
    public final AggregateMetric metric;
    public final SortOrder sortOrder;

    public TopK(Optional<Long> limit, Optional<AggregateMetric> metric, SortOrder sortOrder) {
        Preconditions.checkArgument(limit.isPresent() || metric.isPresent(), "TopK should either have a limit or a metric");
        final AggregateMetric newMetric = metric.or(() -> new AggregateMetric.DocStats(new DocMetric.Count()));

        this.limit = limit;
        this.metric = newMetric;
        this.sortOrder = sortOrder;
    }

    public com.indeed.iql2.execution.commands.misc.TopK toExecution(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.iql2.execution.commands.misc.TopK(
                limit.transform(x -> (int)(long)x),
                metric.toExecutionMetric(namedMetricLookup, groupKeySet),
                sortOrder
        );
    }

    public TopK transformMetric(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupBy) {
            return new TopK(limit, Optional.of(metric.transform(f, g, h, i, groupBy)), sortOrder);
    }

    public TopK traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new TopK(limit, Optional.of(f.apply(metric)), sortOrder);
    }
}
