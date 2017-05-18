package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Query;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SubstituteDimension {
    public static Query substitute(final Query query, final Map<String, DatasetDimensions> dimensionsMetrics) {
        final Map<String, String> datasetAliasToOrigin = query.nameToIndex();
        addDimensionAliasToDatasets(query, dimensionsMetrics);
        final Set<String> datasets = query.datasets.stream().map(d -> d.getDisplayName().unwrap()).collect(Collectors.toSet());
        final Function<String, Optional<DocMetric>> getSubstitutedDimensionMetricFunc = field -> getSubstitutedDimensionDocMetric(datasets, dimensionsMetrics, datasetAliasToOrigin, field);
        return query.transform(
                Functions.identity(),
                substituteDimensionAggregateMetric(dimensionsMetrics, datasetAliasToOrigin),
                substituteDocMetric(getSubstitutedDimensionMetricFunc),
                Functions.identity(),
                substituteDocFilter(getSubstitutedDimensionMetricFunc));
    }

    private static void addDimensionAliasToDatasets(final Query query, final Map<String, DatasetDimensions> dimensionsMetrics) {
        for (Dataset dataset : query.datasets) {
            if (dimensionsMetrics.containsKey(dataset.dataset.unwrap())) {
                final DatasetDimensions datasetDimensions = dimensionsMetrics.get(dataset.dataset.unwrap());
                dataset.setDimensionAlias(datasetDimensions.getUppercaseAliasDimensions());
            }
        }
    }

    private static Function<AggregateMetric, AggregateMetric> substituteDimensionAggregateMetric(
            final Map<String, DatasetDimensions> dimensionsMetrics, final Map<String, String> datasetAliasToOrigin) {
        return new Function<AggregateMetric, AggregateMetric>() {
            @Override
            public AggregateMetric apply(final AggregateMetric input) {
                if (input instanceof AggregateMetric.DocStatsPushes) {
                    final AggregateMetric.DocStatsPushes pushStats = (AggregateMetric.DocStatsPushes) input;
                    final String dataset = datasetAliasToOrigin.get(pushStats.dataset);
                    final DatasetDimensions dimensionMetrics = dimensionsMetrics.get(dataset);
                    final DocMetric docMetric = pushStats.pushes.metric;
                    if (dimensionMetrics != null) {
                        if (docMetric instanceof DocMetric.Field) {
                            final Optional<Dimension> metricDimension = dimensionMetrics.getNonAliasDimension(((DocMetric.Field) docMetric).field);
                            if (metricDimension.isPresent()) {
                                return applyDatasetToExpandedMetric(metricDimension.get().metric, pushStats.dataset);
                            }
                        } else if (docMetric instanceof DocMetric.HasInt) {
                            final Optional<Dimension> dimension = dimensionMetrics.getNonAliasDimension(((DocMetric.HasInt) docMetric).field.unwrap());
                            if (dimension.isPresent()) {
                                final DocMetric dimensionMetric = getDocMetricOrThrow(dimension.get());
                                return new AggregateMetric.DocStatsPushes(
                                        pushStats.dataset,
                                        new DocMetric.PushableDocMetric(
                                                new DocMetric.MetricEqual(dimensionMetric, new DocMetric.Constant(((DocMetric.HasInt) docMetric).term))));
                            }
                        } else {
                            final Function<String, Optional<DocMetric>> getMetricDimensionFunc =
                                    field -> getSubstitutedDimensionDocMetric(ImmutableSet.of(pushStats.dataset), dimensionsMetrics, datasetAliasToOrigin, field);
                            return new AggregateMetric.DocStatsPushes(pushStats.dataset,
                                    new DocMetric.PushableDocMetric(docMetric.transform(
                                            substituteDocMetric(getMetricDimensionFunc),
                                            substituteDocFilter(getMetricDimensionFunc))));
                        }
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocFilter, DocFilter> substituteDocFilter(final Function<String, Optional<DocMetric>> getMetricDimensionFunc) {
        return new Function<DocFilter, DocFilter>() {
            @Nullable
            @Override
            public DocFilter apply(@Nullable final DocFilter input) {
                if (input instanceof DocFilter.FieldTermEqual) {
                    final DocFilter.FieldTermEqual fieldTermEqual = (DocFilter.FieldTermEqual) input;
                    if (fieldTermEqual.term.isIntTerm) {
                        final Optional<DocMetric> metricDimension = getMetricDimensionFunc.apply(fieldTermEqual.field.unwrap());
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
                    final Optional<DocMetric> dimensionMetric = getMetricDimensionFunc.apply(between.field.unwrap());
                    if (dimensionMetric.isPresent()) {
                        return new DocFilter.And(
                                new DocFilter.MetricGte(dimensionMetric.get(), new DocMetric.Constant(between.lowerBound)),
                                new DocFilter.MetricLt(dimensionMetric.get(), new DocMetric.Constant(between.upperBound)));
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocMetric, DocMetric> substituteDocMetric(final Function<String, Optional<DocMetric>> getMetricDimensionFunc) {
        return new Function<DocMetric, DocMetric>() {
            @Nullable
            @Override
            public DocMetric apply(@Nullable final DocMetric input) {
                if (input instanceof DocMetric.Field) {
                    final Optional<DocMetric> dimensionMetric = getMetricDimensionFunc.apply(((DocMetric.Field) input).field);
                    if (dimensionMetric.isPresent()) {
                        return dimensionMetric.get();
                    }
                } else if (input instanceof DocMetric.HasInt) {
                    final Optional<DocMetric> dimensionMetric = getMetricDimensionFunc.apply(((DocMetric.HasInt) input).field.unwrap());
                    return new DocMetric.MetricEqual(dimensionMetric.get(), new DocMetric.Constant(((DocMetric.HasInt) input).term));
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
                if ((input instanceof AggregateMetric.DocStats) || (input instanceof AggregateMetric.ImplicitDocStats)) {
                    final DocMetric docMetric;
                    if (input instanceof AggregateMetric.DocStats) {
                        final AggregateMetric.DocStats stats = (AggregateMetric.DocStats) input;
                        docMetric = stats.metric;
                    } else {
                        final AggregateMetric.ImplicitDocStats implicitDocStats = (AggregateMetric.ImplicitDocStats) input;
                        docMetric = implicitDocStats.docMetric;
                    }
                    return new AggregateMetric.DocStatsPushes(dataset, new DocMetric.PushableDocMetric(docMetric));
                }
                return input;
            }
        }, Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
    }

    // if there is no dimension existed for the field, it will return Optional.absent
    // if dimension metric is not DocMetric, it will throw IllegalArgumentException
    private static Optional<DocMetric> getSubstitutedDimensionDocMetric(
            final Set<String> datasets, final Map<String, DatasetDimensions> dimensionsMetrics, final Map<String, String> datasetAliasToOrigin, final String field) {
        final Map<String, DocMetric> datasetToMetric = Maps.newHashMap();
        boolean foundDimensionMetric = false;

        for (final String dataset : datasets) {
            final String originDataset = datasetAliasToOrigin.get(dataset);
            if (dimensionsMetrics.containsKey(originDataset) && dimensionsMetrics.get(originDataset).getNonAliasDimension(field).isPresent()) {
                final Dimension dimension = dimensionsMetrics.get(originDataset).getNonAliasDimension(field).get();
                datasetToMetric.put(dataset, getDocMetricOrThrow(dimension));
                foundDimensionMetric = true;
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

    private static DocMetric getDocMetricOrThrow(final Dimension dimension) {
        if (!(dimension.metric instanceof AggregateMetric.ImplicitDocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use compound dimensions in per-document context, dimension %s: %s",
                            dimension.name, dimension.expression));
        } else {
            return ((AggregateMetric.ImplicitDocStats) dimension.metric).docMetric;
        }
    }
}
