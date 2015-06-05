package com.indeed.jql.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.precomputed.Precomputed;
import com.indeed.jql.language.query.GroupBy;
import com.indeed.jql.language.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ExtractPrecomputed {
    public static Extracted extractPrecomputed(Query query) {
        final Processor processor = new Processor(1, 1, query.extractDatasetNames());
        final List<GroupBy> groupBys = new ArrayList<>();
        for (int i = 0; i < query.groupBys.size(); i++) {
            final GroupBy groupBy = query.groupBys.get(i);
            processor.setDepth(i + 1);
            processor.setStartDepth(i + 1);
            groupBys.add(groupBy.traverse1(processor));
        }
        final List<AggregateMetric> selects = new ArrayList<>();
        processor.setDepth(groupBys.size());
        for (int i = 0; i < query.selects.size(); i++) {
            final AggregateMetric select = query.selects.get(i);
            selects.add(processor.apply(select));
        }
        return new Extracted(new Query(query.datasets, query.filter, groupBys, selects));
    }

    private static class Processor implements Function<AggregateMetric, AggregateMetric> {
        private int depth;
        private int startDepth;
        private Set<String> scope;

        public Processor(int depth, int startDepth, Set<String> scope) {
            this.depth = depth;
            this.startDepth = startDepth;
            this.scope = scope;
        }

        public AggregateMetric apply(AggregateMetric input) {
            if (input instanceof AggregateMetric.Parent) {
                final AggregateMetric.Parent parent = (AggregateMetric.Parent) input;
                final int prevDepth = this.depth;
                this.setDepth(prevDepth - 1);
                final AggregateMetric result = apply(parent.metric);
                this.setDepth(prevDepth);
                return result;
            } else if (input instanceof AggregateMetric.Distinct) {
                final AggregateMetric.Distinct distinct = (AggregateMetric.Distinct) input;
                final Optional<AggregateFilter> filter;
                if (distinct.filter.isPresent()) {
                    final int prevDepth = this.depth;
                    this.setDepth(prevDepth + 1);
                    final int prevStartDepth = this.startDepth;
                    this.setStartDepth(startDepth + 1);
                    filter = Optional.of(distinct.filter.get().traverse1(this));
                    this.setDepth(prevDepth);
                    this.setStartDepth(prevStartDepth);
                } else {
                    filter = Optional.absent();
                }
                return handlePrecomputed(new Precomputed.PrecomputedDistinct(distinct.field, filter, distinct.windowSize));
            } else if (input instanceof AggregateMetric.Percentile) {
                final AggregateMetric.Percentile percentile = (AggregateMetric.Percentile) input;
                return handlePrecomputed(new Precomputed.PrecomputedPercentile(percentile.field, percentile.percentile));
            } else if (input instanceof AggregateMetric.Qualified) {
                final AggregateMetric.Qualified qualified = (AggregateMetric.Qualified) input;
                final Set<String> oldScope = ImmutableSet.copyOf(this.scope);
                setScope(Sets.newHashSet(qualified.scope));
                final AggregateMetric result = apply(qualified.metric);
                setScope(oldScope);
                return result;
            } else if (input instanceof AggregateMetric.DocStats || input instanceof AggregateMetric.ImplicitDocStats) {
                final DocMetric docMetric;
                if (input instanceof AggregateMetric.DocStats) {
                    final AggregateMetric.DocStats stats = (AggregateMetric.DocStats) input;
                    docMetric = stats.metric;
                } else {
                    final AggregateMetric.ImplicitDocStats implicitDocStats = (AggregateMetric.ImplicitDocStats) input;
                    docMetric = new DocMetric.Field(implicitDocStats.field);
                }
                if (startDepth == depth) {
                    AggregateMetric aggregateMetric = null;
                    for (final String dataset : scope) {
                        final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(dataset, docMetric.getPushes(dataset));
                        if (aggregateMetric == null) {
                            aggregateMetric = metric;
                        } else {
                            aggregateMetric = new AggregateMetric.Add(metric, aggregateMetric);
                        }
                    }
                    return aggregateMetric;
                } else {
                    return handlePrecomputed(new Precomputed.PrecomputedRawStats(docMetric));
                }
            } else {
                return input.traverse1(this);
            }
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public void setStartDepth(int startDepth) {
            this.startDepth = startDepth;
        }

        private AggregateMetric handlePrecomputed(Precomputed precomputed) {
            throw new UnsupportedOperationException();
        }

        public void setScope(Set<String> scope) {
            this.scope = scope;
        }
    }

    public static class Extracted {
        public final Query query;

        private Extracted(Query query) {
            this.query = query;
        }
    }
}
