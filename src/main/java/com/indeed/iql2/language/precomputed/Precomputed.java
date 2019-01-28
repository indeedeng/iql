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

package com.indeed.iql2.language.precomputed;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.commands.ComputeFieldExtremeValue;
import com.indeed.iql2.language.commands.GetGroupDistincts;
import com.indeed.iql2.language.commands.GetGroupPercentiles;
import com.indeed.iql2.language.commands.GetGroupStats;
import com.indeed.iql2.language.commands.GroupLookupMergeType;
import com.indeed.iql2.language.commands.RegroupIntoParent;
import com.indeed.iql2.language.commands.SumAcross;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.Optionals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface Precomputed {
    Precomputation commands(Set<String> scope);
    Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);
    Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f);

    class PrecomputedDistinct implements Precomputed {
        public final FieldSet field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public PrecomputedDistinct(FieldSet field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupDistincts(field, filter, windowSize.or(1)));
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(new PrecomputedDistinct(field, Optionals.transform(filter, f, g, h, i, groupByFunction), windowSize));
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.absent();
            }
            return new PrecomputedDistinct(field, filter, windowSize);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedDistinct that = (PrecomputedDistinct) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(windowSize, that.windowSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, filter, windowSize);
        }

        @Override
        public String toString() {
            return "PrecomputedDistinct{" +
                    "field='" + field + '\'' +
                    ", filter=" + filter +
                    ", windowSize=" + windowSize +
                    '}';
        }
    }

    class PrecomputedPercentile implements Precomputed {
        public final FieldSet field;
        public final double percentile;

        public PrecomputedPercentile(FieldSet field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupPercentiles(field, new double[]{percentile}));
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(this);
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedPercentile that = (PrecomputedPercentile) o;
            return Objects.equals(percentile, that.percentile) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, percentile);
        }

        @Override
        public String toString() {
            return "PrecomputedPercentile{" +
                    "field='" + field + '\'' +
                    ", percentile=" + percentile +
                    '}';
        }
    }

    class PrecomputedRawStats implements Precomputed {
        public final DocMetric docMetric;

        public PrecomputedRawStats(DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public Precomputation commands(final Set<String> scope) {
            final List<AggregateMetric> metrics = new ArrayList<>(scope.size());
            for (final String dataset : scope) {
                final AggregateMetric metric = new AggregateMetric.DocStatsPushes(dataset, docMetric);
                metrics.add(metric);
            }
            final AggregateMetric metric = AggregateMetric.Add.create(metrics);
            return Precomputation.noContext(new GetGroupStats(Collections.singletonList(metric), Collections.singletonList(Optional.<String>absent()), false));
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(new PrecomputedRawStats(docMetric.transform(g, i)));
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedRawStats that = (PrecomputedRawStats) o;
            return Objects.equals(docMetric, that.docMetric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docMetric);
        }

        @Override
        public String toString() {
            return "PrecomputedRawStats{" +
                    "docMetric=" + docMetric +
                    '}';
        }
    }

    class PrecomputedSumAcross implements Precomputed {
        public final FieldSet field;
        public final AggregateMetric metric;
        public final Optional<AggregateFilter> filter;

        public PrecomputedSumAcross(FieldSet field, AggregateMetric metric, Optional<AggregateFilter> filter) {
            this.field = field;
            this.metric = metric;
            this.filter = filter;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            Preconditions.checkState(scope.equals(field.datasets()));
            return Precomputation.noContext(new SumAcross(field, metric, filter));
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(new PrecomputedSumAcross(field, metric.transform(f, g, h, i, groupByFunction), Optionals.transform(filter, f, g, h, i, groupByFunction)));
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new PrecomputedSumAcross(field, f.apply(metric), Optionals.traverse1(filter, f));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedSumAcross that = (PrecomputedSumAcross) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(metric, that.metric) &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, metric, filter);
        }

        @Override
        public String toString() {
            return "PrecomputedSumAcross{" +
                    "field='" + field + '\'' +
                    ", metric=" + metric +
                    ", filter=" + filter +
                    '}';
        }
    }

    class PrecomputedSumAcrossGroupBy implements Precomputed {
        public final GroupBy groupBy;
        public final AggregateMetric metric;

        public PrecomputedSumAcrossGroupBy(GroupBy groupBy, AggregateMetric metric) {
            this.groupBy = groupBy;
            this.metric = metric;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return new Precomputation(
                    groupBy.executionStep(scope).commands(),
                    new GetGroupStats(Collections.singletonList(metric), Collections.singletonList(Optional.<String>absent()), false),
                    Collections.<Command>singletonList(new RegroupIntoParent(GroupLookupMergeType.SumAll))
            );
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(new PrecomputedSumAcrossGroupBy(groupBy.transform(groupByFunction, f, g, h, i), metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new PrecomputedSumAcrossGroupBy(groupBy.traverse1(f), metric.traverse1(f));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedSumAcrossGroupBy that = (PrecomputedSumAcrossGroupBy) o;
            return Objects.equals(groupBy, that.groupBy) &&
                    Objects.equals(metric, that.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupBy, metric);
        }

        @Override
        public String toString() {
            return "PrecomputedSumAcrossGroupBy{" +
                    "groupBy=" + groupBy +
                    ", metric=" + metric +
                    '}';
        }
    }

    class PrecomputedFieldExtremeValue implements Precomputed {
        public final FieldSet field;
        public final AggregateMetric metric;
        public final Optional<AggregateFilter> filter;

        public PrecomputedFieldExtremeValue(
                final FieldSet field,
                final AggregateMetric metric,
                final Optional<AggregateFilter> filter
                ) {
            this.field = field;
            this.metric = metric;
            this.filter = filter;
        }

        @Override
        public Precomputation commands(final Set<String> scope) {
            Preconditions.checkState(scope.equals(field.datasets()));
            return Precomputation.noContext(new ComputeFieldExtremeValue(field, metric, filter));
        }

        @Override
        public Precomputed transform(
                final Function<Precomputed, Precomputed> precomputed,
                final Function<AggregateMetric, AggregateMetric> f,
                final Function<DocMetric, DocMetric> g,
                final Function<AggregateFilter, AggregateFilter> h,
                final Function<DocFilter, DocFilter> i,
                final Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(
                new PrecomputedFieldExtremeValue(field,
                    metric.transform(f, g, h, i, groupByFunction),
                    filter.transform(fil -> fil.transform(f, g, h, i, groupByFunction))
                )
            );
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new PrecomputedFieldExtremeValue(field, f.apply(metric), Optionals.traverse1(filter, f));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedFieldExtremeValue that = (PrecomputedFieldExtremeValue) o;
            return Objects.equals(field, that.field) &&
                Objects.equals(metric, that.metric) &&
                Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, metric, filter);
        }

        @Override
        public String toString() {
            return "PrecomputedFieldExtremeValue{" +
                    "field='" + field + '\'' +
                    ", metric='" + metric + '\'' +
                    ", filter='" + filter + '\'' +
                    '}';
        }
    }

    class Precomputation {
        public final List<Command> beforeCommands;
        public final Command computationCommand;
        public final List<Command> afterCommands;

        public Precomputation(List<Command> beforeCommands, Command computationCommand, List<Command> afterCommands) {
            this.beforeCommands = beforeCommands;
            this.computationCommand = computationCommand;
            this.afterCommands = afterCommands;
        }

        public static Precomputation noContext(Command command) {
            return new Precomputation(Collections.<Command>emptyList(), command, Collections.<Command>emptyList());
        }
    }
}
