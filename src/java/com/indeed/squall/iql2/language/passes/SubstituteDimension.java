package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;

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
                        final Map<String, Dimension> fieldToDimension = datasetsMetadata.getMetadata(dataset).get().fieldToDimension;
                        if (docMetric instanceof DocMetric.Field) {
                            final Dimension metricDimension = fieldToDimension.get(((DocMetric.Field) docMetric).field);
                            if ((metricDimension != null) && !metricDimension.isAlias) {
                                return applyDatasetToExpandedMetric(metricDimension.metric, pushStats.dataset);
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
                    final DocMetric docMetric;
                    final AggregateMetric.DocStats docStats = (AggregateMetric.DocStats) input;
                    docMetric = docStats.docMetric;
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
                final Dimension dimension = datasetsMetadata.getMetadata(originDataset).get().fieldToDimension.get(field);
                if (!dimension.isAlias) {
                    datasetToMetric.put(dataset, getDocMetricOrThrow(dimension));
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

    private static DocMetric getDocMetricOrThrow(final Dimension dimension) {
        if (!(dimension.metric instanceof AggregateMetric.DocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use compound metrics in per-document context, metric [ %s: %s ]",
                            dimension.name, dimension.expression));
        } else {
            return ((AggregateMetric.DocStats) dimension.metric).docMetric;
        }
    }
}
