package com.indeed.jql.language.precomputed;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.commands.Command;
import com.indeed.jql.language.commands.GetGroupDistincts;
import com.indeed.jql.language.commands.GetGroupPercentiles;
import com.indeed.jql.language.commands.GetGroupStats;
import com.indeed.jql.language.commands.GroupLookupMergeType;
import com.indeed.jql.language.commands.RegroupIntoParent;
import com.indeed.jql.language.commands.SumAcross;
import com.indeed.jql.language.query.GroupBy;
import com.indeed.jql.language.util.Optionals;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface Precomputed {
    Precomputation commands(Set<String> scope);
    Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);
    Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f);

    class PrecomputedDistinct implements Precomputed {
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public PrecomputedDistinct(String field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupDistincts(scope, field, filter, windowSize.or(1)));
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
        public final String field;
        public final double percentile;

        public PrecomputedPercentile(String field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupPercentiles(scope, field, new double[]{percentile}));
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
        public Precomputation commands(Set<String> scope) {
            AggregateMetric metric = null;
            for (final String dataset : scope) {
                final AggregateMetric aMetric = new AggregateMetric.DocStatsPushes(dataset, docMetric.getPushes(dataset));
                if (metric == null) {
                    metric = aMetric;
                } else {
                    metric = new AggregateMetric.Add(aMetric, metric);
                }
            }
            return Precomputation.noContext(new GetGroupStats(Collections.singletonList(metric), false));
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
        public final String field;
        public final AggregateMetric metric;
        public final Optional<AggregateFilter> filter;

        public PrecomputedSumAcross(String field, AggregateMetric metric, Optional<AggregateFilter> filter) {
            this.field = field;
            this.metric = metric;
            this.filter = filter;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new SumAcross(scope, field, metric, filter));
        }

        @Override
        public Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return precomputed.apply(new PrecomputedSumAcross(field, metric.transform(f, g, h, i, groupByFunction), Optionals.transform(filter, f, g, h, i, groupByFunction)));
        }

        @Override
        public Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new PrecomputedSumAcross(field, f.apply(metric), Optionals.traverse1(filter, f));
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
                    new GetGroupStats(Collections.singletonList(metric), false),
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
