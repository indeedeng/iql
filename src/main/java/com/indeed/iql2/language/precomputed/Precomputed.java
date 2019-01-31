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
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.Optionals;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface Precomputed {
    Precomputation commands(List<Dataset> datasets);
    Precomputed transform(Function<Precomputed, Precomputed> precomputed, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);
    Precomputed traverse1(Function<AggregateMetric, AggregateMetric> f);

    @Data
    class PrecomputedDistinct implements Precomputed {
        public final FieldSet field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        @Override
        public Precomputation commands(List<Dataset> datasets) {
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
    }

    @Data
    class PrecomputedPercentile implements Precomputed {
        public final FieldSet field;
        public final double percentile;

        @Override
        public Precomputation commands(List<Dataset> datasets) {
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
    }

    @Data
    class PrecomputedRawStats implements Precomputed {
        public final DocMetric docMetric;

        @Override
        public Precomputation commands(final List<Dataset> datasets) {
            final List<AggregateMetric> metrics = new ArrayList<>(datasets.size());
            Set<String> scope = Dataset.datasetToScope(datasets);
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
    }

    @Data
    class PrecomputedSumAcross implements Precomputed {
        public final FieldSet field;
        public final AggregateMetric metric;
        public final Optional<AggregateFilter> filter;

        @Override
        public Precomputation commands(List<Dataset> datasets) {
            Preconditions.checkState(Dataset.datasetToScope(datasets).equals(field.datasets()));
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
    }

    @Data
    class PrecomputedSumAcrossGroupBy implements Precomputed {
        public final GroupBy groupBy;
        public final AggregateMetric metric;

        @Override
        public Precomputation commands(List<Dataset> datasets) {
            return new Precomputation(
                    groupBy.executionStep(datasets).commands(),
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
    }

    @Data
    class PrecomputedFieldExtremeValue implements Precomputed {
        public final FieldSet field;
        public final AggregateMetric metric;
        public final Optional<AggregateFilter> filter;

        @Override
        public Precomputation commands(final List<Dataset> datasets) {
            Preconditions.checkState(Dataset.datasetToScope(datasets).equals(field.datasets()));
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
    }

    @Data
    class Precomputation {
        public final List<Command> beforeCommands;
        public final Command computationCommand;
        public final List<Command> afterCommands;

        public static Precomputation noContext(Command command) {
            return new Precomputation(Collections.<Command>emptyList(), command, Collections.<Command>emptyList());
        }
    }
}
