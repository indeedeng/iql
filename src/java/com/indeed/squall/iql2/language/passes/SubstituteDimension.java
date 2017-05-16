package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Query;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SubstituteDimension {
    public static Query substitute(final Query query, final Map<String, DatasetDimensions> dimensionsMetrics) {
        final Map<String, String> datasetAliasToOrigin = query.nameToIndex();
        addDimensionAliasToDatasets(query, dimensionsMetrics);
        return query.transform(
                Functions.identity(),
                substituteDimensionAggregateMetric(dimensionsMetrics, datasetAliasToOrigin),
                substituteDocMetric(field -> getAndValidateDatasetDimension(query.datasets, dimensionsMetrics, field)),
                Functions.identity(),
                substituteDocFilter(field -> getAndValidateDatasetDimension(query.datasets, dimensionsMetrics, field)));
    }

    private static void addDimensionAliasToDatasets(final Query query, final Map<String, DatasetDimensions> dimensionsMetrics) {
        for (Dataset dataset : query.datasets) {
            if (dimensionsMetrics.containsKey(dataset.dataset.unwrap())) {
                final DatasetDimensions datasetDimensions = dimensionsMetrics.get(dataset.dataset.unwrap());
                dataset.setDimensionAlias(datasetDimensions.getUppercaseAliasDimensions());
            }
        }
    }

    private static Function<AggregateMetric, AggregateMetric> substituteDimensionAggregateMetric(final Map<String, DatasetDimensions> dimensionsMetrics, final Map<String, String> datasetAliasToOrigin) {
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
                            final Optional<Dimension> metricDimension = dimensionMetrics.getNonAliasDimension(((DocMetric.HasInt) docMetric).field.unwrap());
                            if (metricDimension.isPresent()) {
                                return new AggregateMetric.DocStatsPushes(
                                        pushStats.dataset,
                                        new DocMetric.PushableDocMetric(substituteHasInt(((DocMetric.HasInt) docMetric), metricDimension.get())));
                            }
                        } else {
                            return new AggregateMetric.DocStatsPushes(pushStats.dataset,
                                    new DocMetric.PushableDocMetric(docMetric.transform(substituteDocMetric(dimensionMetrics::getNonAliasDimension), substituteDocFilter(dimensionMetrics::getNonAliasDimension))));
                        }
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocFilter, DocFilter> substituteDocFilter(final Function<String, Optional<Dimension>> getMetricDimensionFunc) {
        return new Function<DocFilter, DocFilter>() {
            @Nullable
            @Override
            public DocFilter apply(@Nullable final DocFilter input) {
                if (input instanceof DocFilter.FieldTermEqual) {
                    final DocFilter.FieldTermEqual fieldTermEqual = (DocFilter.FieldTermEqual) input;
                    if (fieldTermEqual.term.isIntTerm) {
                        final Optional<Dimension> metricDimension = getMetricDimensionFunc.apply(fieldTermEqual.field.unwrap());
                        if (metricDimension.isPresent()) {
                            final DocMetric dimensionDocMetric = getNonAliasDimensionImplicitDocMetricOrThrow(metricDimension.get());
                            if (fieldTermEqual.equal) {
                                return new DocFilter.MetricEqual(dimensionDocMetric, new DocMetric.Constant(fieldTermEqual.term.intTerm));
                            } else {
                                return new DocFilter.MetricNotEqual(dimensionDocMetric, new DocMetric.Constant(fieldTermEqual.term.intTerm));
                            }
                        }
                    }
                }
                return input;
            }
        };
    }

    private static Function<DocMetric, DocMetric> substituteDocMetric(final Function<String, Optional<Dimension>> getMetricDimensionFunc) {
        return new Function<DocMetric, DocMetric>() {
            @Nullable
            @Override
            public DocMetric apply(@Nullable final DocMetric input) {
                if (input instanceof DocMetric.Field) {
                    final Optional<Dimension> metricDimension = getMetricDimensionFunc.apply(((DocMetric.Field) input).field);
                    if (metricDimension.isPresent()) {
                        return getNonAliasDimensionImplicitDocMetricOrThrow(metricDimension.get());
                    }
                } else if (input instanceof DocMetric.HasInt) {
                    final Optional<Dimension> metricDimension = getMetricDimensionFunc.apply(((DocMetric.HasInt) input).field.unwrap());
                    if (metricDimension.isPresent()) {
                        return substituteHasInt((DocMetric.HasInt) input, metricDimension.get());
                    }
                }
                return input;
            }
        };
    }

    private static DocMetric substituteHasInt(final DocMetric.HasInt docMetric, final Dimension dimension) {
        if (!(dimension.metric instanceof AggregateMetric.ImplicitDocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use aggregate dimensions in per-document context, dimension %s: %s",
                            dimension.name, dimension.expression));
        } else {
            return new DocMetric.MetricEqual(
                    ((AggregateMetric.ImplicitDocStats) dimension.metric).docMetric,
                    new DocMetric.Constant(docMetric.term));
        }
    }

    private static DocMetric getNonAliasDimensionImplicitDocMetricOrThrow(final Dimension dimension) {
        if (!(dimension.metric instanceof AggregateMetric.ImplicitDocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use aggregate dimensions in per-document context, dimension %s: %s",
                            dimension.name, dimension.expression));
        } else {
            return ((AggregateMetric.ImplicitDocStats) dimension.metric).docMetric;
        }
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

    // this is to ensure that the dimension should either be existed in all datasets or none
    private static Optional<Dimension> getAndValidateDatasetDimension(
            final List<Dataset> datasetInfos, final Map<String, DatasetDimensions> dimensionsMetrics, final String field) {
        final List<String> datasets = datasetInfos.stream().map(d -> d.dataset.unwrap()).collect(Collectors.toList());
        Optional<Dimension> result = null;
        for (final Dataset datasetInfo : datasetInfos) {
            final String dataset = datasetInfo.dataset.unwrap();
            final DatasetDimensions datasetDimensions = dimensionsMetrics.get(dataset);
            if (datasetDimensions == null) {
                if (result == null) {
                    result = Optional.absent();
                } else if (result.equals(Optional.absent())) {
                    throw new IllegalArgumentException(
                            String.format("metric %s not found in all datasets: %s", field, Joiner.on(",").join(datasets)));
                }
            } else {
                final Optional<Dimension> dimension = datasetDimensions.getNonAliasDimension(field);
                if (!dimension.isPresent()) {
                    if (result == null) {
                        result = Optional.absent();
                    } else if (!result.equals(Optional.absent())) {
                        throw new IllegalArgumentException(
                                String.format("metric %s not found in dataset: %s", field, dataset));
                    }
                } else {
                    if (result == null) {
                        result = dimension;
                    } else if (result.equals(Optional.absent())) {
                        throw new IllegalArgumentException(
                                String.format("metric %s not found in all datasets: %s", field, Joiner.on(",").join(datasets)));
                    } else if (!result.equals(dimension)) {
                        throw new IllegalArgumentException(
                                String.format("definition of metric %s is not the same in all datasets: %s != %s",
                                        field, result.get().expression, dimension.get().expression));
                    }
                }
            }
        }
        return result;
    }
}
