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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.SortOrder;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.GroupBy;

import java.util.Optional;
import java.util.function.Function;

@EqualsAndHashCode
@ToString
public class TopK {
    public final Optional<Long> limit;
    public final AggregateMetric metric;
    public final SortOrder sortOrder;

    public TopK(final Optional<Long> limit, final Optional<AggregateMetric> metric, final SortOrder sortOrder) {
        Preconditions.checkArgument(limit.isPresent() || metric.isPresent(), "TopK should either have a limit or a metric");
        final AggregateMetric newMetric = metric.orElse(new AggregateMetric.DocStats(new DocMetric.Count()));

        this.limit = limit;
        this.metric = newMetric;
        this.sortOrder = sortOrder;
    }

    public com.indeed.iql2.execution.commands.misc.TopK toExecution(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
        return new com.indeed.iql2.execution.commands.misc.TopK(
                limit.map(x -> (int)(long)x),
                metric.toExecutionMetric(namedMetricLookup, groupKeySet),
                sortOrder
        );
    }

    public TopK transformMetric(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupBy) {
            return new TopK(limit, Optional.of(metric.transform(f, g, h, i, groupBy)), sortOrder);
    }

    public TopK traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new TopK(limit, Optional.of(f.apply(metric)), sortOrder);
    }

    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        metric.validate(validationHelper.datasets(), validationHelper, errorCollector);
        if (limit.isPresent()) {
            final long limitValue = limit.get();
            // arbitrarily subtracting 1 because boundaries are easy to get screwy and this number is
            // already too massive to be reasonable.
            final int groupLimit = Objects.firstNonNull(validationHelper.limits.queryInMemoryRowsLimit, 1_000_000);
            if ((limitValue <= 0) || (limitValue > groupLimit)) {
                errorCollector.error("The K in Top K must be in [1, " + groupLimit + "). Value was: " + limitValue);
            }
        }
    }
}
