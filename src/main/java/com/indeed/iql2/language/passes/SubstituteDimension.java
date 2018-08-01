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

package com.indeed.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.indeed.iql.metadata.MetricMetadata;
import com.indeed.iql2.language.metadata.DatasetsMetadata;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.Query;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SubstituteDimension {
    public static Query substitute(final Query query, final DatasetsMetadata datasetsMetadata) {
        final Map<String, String> datasetAliasToOriginal = query.nameToIndex();
        final Set<String> datasets = query.datasets.stream().map(d -> d.getDisplayName().unwrap()).collect(Collectors.toSet());
        final Function<String, Optional<DocMetric>> getSubstitutedDimensionMetricFunc = field -> getSubstitutedDimensionDocMetric(datasets, datasetsMetadata, datasetAliasToOriginal, field);
        return query.transform(
                Functions.identity(),
                substituteDimensionAggregateMetric(datasetsMetadata, datasetAliasToOriginal),
                substituteDocMetric(getSubstitutedDimensionMetricFunc),
                Functions.identity(),
                substituteDocFilter(getSubstitutedDimensionMetricFunc));
    }

    public static Function<AggregateMetric, AggregateMetric> substituteDimensionAggregateMetric(final Query query, final DatasetsMetadata datasetsMetadata) {
        final Map<String, String> datasetAliasToOriginal = query.nameToIndex();
        return substituteDimensionAggregateMetric(datasetsMetadata, datasetAliasToOriginal);
    }

    private static Function<AggregateMetric, AggregateMetric> substituteDimensionAggregateMetric(
            final DatasetsMetadata datasetsMetadata, final Map<String, String> datasetAliasToOrigin) {
        return new Function<AggregateMetric, AggregateMetric>() {
            @Override
            public AggregateMetric apply(final AggregateMetric input) {
                Preconditions.checkArgument(!(input instanceof AggregateMetric.DocStats), "DocStats should be handled by ExtractPrecomputed already");
                if (input instanceof AggregateMetric.DocStatsPushes) {
                    final AggregateMetric.DocStatsPushes pushStats = (AggregateMetric.DocStatsPushes) input;
                    final String dataset = datasetAliasToOrigin.get(pushStats.dataset);
                    final DocMetric docMetric = pushStats.pushes.metric;
                    if (datasetsMetadata.getMetadata(dataset).isPresent()) {
                        final Map<String, MetricMetadata> fieldToDimension = datasetsMetadata.getMetadata(dataset).get().fieldToDimension;
                        if (docMetric instanceof DocMetric.Field) {
                            final MetricMetadata metricMetadata = fieldToDimension.get(((DocMetric.Field) docMetric).field);
                            if ((metricMetadata != null) && !metricMetadata.isAlias) {
                                return applyDatasetToExpandedMetric(metricMetadata.metric, pushStats.dataset);
                            }
                        } else {
                            final Function<String, Optional<DocMetric>> getDimensionMetricFunc =
                                    field -> getSubstitutedDimensionDocMetric(ImmutableSet.of(pushStats.dataset), datasetsMetadata, datasetAliasToOrigin, field);
                            return new AggregateMetric.DocStatsPushes(pushStats.dataset,
                                    new DocMetric.PushableDocMetric(docMetric.transform(
                                            substituteDocMetric(getDimensionMetricFunc),
                                            substituteDocFilter(getDimensionMetricFunc))));
                        }
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocFilter, DocFilter> substituteDocFilter(final Function<String, Optional<DocMetric>> getDimensionMetricFunc) {
        return new Function<DocFilter, DocFilter>() {
            @Nullable
            @Override
            public DocFilter apply(@Nullable final DocFilter input) {
                if (input instanceof DocFilter.FieldTermEqual) {
                    final DocFilter.FieldTermEqual fieldTermEqual = (DocFilter.FieldTermEqual) input;
                    if (fieldTermEqual.term.isIntTerm) {
                        final Optional<DocMetric> metricDimension = getDimensionMetricFunc.apply(fieldTermEqual.field.unwrap());
                        if (metricDimension.isPresent()) {
                            final DocMetric dimensionMetric = metricDimension.get();
                            if (fieldTermEqual.equal) {
                                return new DocFilter.MetricEqual(dimensionMetric, new DocMetric.Constant(fieldTermEqual.term.intTerm));
                            } else {
                                return new DocFilter.MetricNotEqual(dimensionMetric, new DocMetric.Constant(fieldTermEqual.term.intTerm));
                            }
                        }
                    }
                } else if (input instanceof DocFilter.Between) {
                    final DocFilter.Between between = (DocFilter.Between) input;
                    final Optional<DocMetric> dimensionMetric = getDimensionMetricFunc.apply(between.field.unwrap());
                    if (dimensionMetric.isPresent()) {
                        return new DocFilter.And(
                                new DocFilter.MetricGte(dimensionMetric.get(), new DocMetric.Constant(between.lowerBound)),
                                new DocFilter.MetricLt(dimensionMetric.get(), new DocMetric.Constant(between.upperBound)));
                    }
                } else if (input instanceof DocFilter.FieldEqual) {
                    final DocFilter.FieldEqual fieldEqualFilter = ((DocFilter.FieldEqual) input);
                    final Optional<DocMetric> dimensionMetric1 = getDimensionMetricFunc.apply(fieldEqualFilter.field1.unwrap());
                    final Optional<DocMetric> dimensionMetric2 = getDimensionMetricFunc.apply(fieldEqualFilter.field2.unwrap());
                    if (dimensionMetric1.isPresent() || dimensionMetric2.isPresent()) {
                        return new DocFilter.MetricEqual(
                                dimensionMetric1.or(new DocMetric.Field(fieldEqualFilter.field1)),
                                dimensionMetric2.or(new DocMetric.Field(fieldEqualFilter.field2)));
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocMetric, DocMetric> substituteDocMetric(final Function<String, Optional<DocMetric>> getDimensionMetricFunc) {
        return new Function<DocMetric, DocMetric>() {
            @Nullable
            @Override
            public DocMetric apply(@Nullable final DocMetric input) {
                if (input instanceof DocMetric.Field) {
                    final Optional<DocMetric> dimensionMetric = getDimensionMetricFunc.apply(((DocMetric.Field) input).field);
                    if (dimensionMetric.isPresent()) {
                        return dimensionMetric.get();
                    }
                } else if (input instanceof DocMetric.HasInt) {
                    final Optional<DocMetric> dimensionMetric = getDimensionMetricFunc.apply(((DocMetric.HasInt) input).field.unwrap());
                    if (dimensionMetric.isPresent()) {
                        return new DocMetric.MetricEqual(dimensionMetric.get(), new DocMetric.Constant(((DocMetric.HasInt) input).term));
                    }
                } else if (input instanceof DocMetric.FieldEqualMetric) {
                    DocMetric.FieldEqualMetric fieldEqualMetric = ((DocMetric.FieldEqualMetric) input);
                    final Optional<DocMetric> dimensionMetric1 = getDimensionMetricFunc.apply(fieldEqualMetric.field1.unwrap());
                    final Optional<DocMetric> dimensionMetric2 = getDimensionMetricFunc.apply(fieldEqualMetric.field2.unwrap());
                    if (dimensionMetric1.isPresent() || dimensionMetric2.isPresent()) {
                        return new DocMetric.MetricEqual(
                                dimensionMetric1.or(new DocMetric.Field(fieldEqualMetric.field1)),
                                dimensionMetric2.or(new DocMetric.Field(fieldEqualMetric.field2)));
                    }
                }
                return input;
            }
        };
    }

    private static AggregateMetric applyDatasetToExpandedMetric(final AggregateMetric expandedMetric, final String dataset) {
        return expandedMetric.transform(new Function<AggregateMetric, AggregateMetric>() {
            @Nullable
            @Override
            public AggregateMetric apply(@Nullable final AggregateMetric input) {
                if (input instanceof AggregateMetric.DocStats) {
                    final AggregateMetric.DocStats docStats = (AggregateMetric.DocStats) input;
                    final DocMetric docMetric = docStats.docMetric;
                    return new AggregateMetric.DocStatsPushes(dataset, new DocMetric.PushableDocMetric(docMetric));
                }
                return input;
            }
        }, Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
    }

    // if there is no dimension existed for the field, it will return Optional.absent
    // if dimension metric is not DocMetric, it will throw IllegalArgumentException
    private static Optional<DocMetric> getSubstitutedDimensionDocMetric(
            final Set<String> datasets, final DatasetsMetadata datasetsMetadata, final Map<String, String> datasetAliasToOrigin, final String field) {
        final Map<String, DocMetric> datasetToMetric = Maps.newHashMap();
        boolean foundDimensionMetric = false;

        for (final String dataset : datasets) {
            final String originDataset = datasetAliasToOrigin.get(dataset).toUpperCase();
            if (datasetsMetadata.getMetadata(originDataset).isPresent() &&
                    datasetsMetadata.getMetadata(originDataset).get().fieldToDimension.containsKey(field)) {
                final MetricMetadata metricMetadata = datasetsMetadata.getMetadata(originDataset).get().fieldToDimension.get(field);
                if (!metricMetadata.isAlias) {
                    datasetToMetric.put(dataset, getDocMetricOrThrow(metricMetadata));
                    foundDimensionMetric = true;
                } else {
                    datasetToMetric.put(dataset, new DocMetric.Field(field));
                }
            } else {
                datasetToMetric.put(dataset, new DocMetric.Field(field));
            }
        }
        if (!foundDimensionMetric) {
            return Optional.absent();
        } else {
            if (datasets.size() == 1) {
                return Optional.of(Iterables.getLast(datasetToMetric.values()));
            } else {
                return Optional.of(new DocMetric.PerDatasetDocMetric(datasetToMetric));
            }
        }
    }

    private static DocMetric getDocMetricOrThrow(final MetricMetadata metricMetadata) {
        if (!(metricMetadata.metric instanceof AggregateMetric.DocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use compound metrics in per-document context, metric [ %s: %s ]",
                            metricMetadata.name, metricMetadata.expression));
        } else {
            return ((AggregateMetric.DocStats) metricMetadata.metric).docMetric;
        }
    }
}
