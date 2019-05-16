package com.indeed.iql2.language.query.fieldresolution;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql.metadata.MetricMetadata;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetrics;
import com.indeed.iql2.language.Identifiers;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.passes.SubstituteDimension;
import com.indeed.iql2.language.query.Query;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author jwolfe
 */
public class ScopedFieldResolver {
    private static final Logger log = Logger.getLogger(ScopedFieldResolver.class);

    private final FieldResolver fieldResolver;
    private final Set<String> scope;

    public ScopedFieldResolver(final FieldResolver fieldResolver, final Set<String> scope) {
        this.fieldResolver = fieldResolver;
        Preconditions.checkArgument(!scope.isEmpty(), "Field resolver scope must always be non-empty");
        this.scope = scope;
    }

    public Positioned<String> resolveImhotepDataset(final JQLParser.IdentifierContext ctx) {
        final String dataset = fieldResolver.datasetsMetadata.resolveDatasetName(Identifiers.extractIdentifier(ctx));
        return Positioned.from(dataset, ctx);
    }

    public Positioned<String> resolveDataset(final JQLParser.IdentifierContext datasetCtx) {
        return Positioned.from(resolveDataset(Identifiers.extractIdentifier(datasetCtx)), datasetCtx);
    }

    public String resolveDataset(final String dataset) {
        final List<String> allMatching = scope.stream().filter(x -> x.equalsIgnoreCase(dataset)).collect(Collectors.toList());
        // Prefer exact match.
        if (allMatching.contains(dataset)) {
            return dataset;
        }
        // Ambiguity
        if (allMatching.size() > 1) {
            fieldResolver.addError(new IqlKnownException.UnknownDatasetException("Multiple datasets match, and none are an exact match: " + allMatching + ", seeking \"" + dataset + "\""));
            return FieldResolver.FAILED_TO_RESOLVE_DATASET;
        }
        if (allMatching.isEmpty()) {
            fieldResolver.addError(new IqlKnownException.UnknownDatasetException("Dataset not found or not included in query: \"" + dataset + "\""));
            return FieldResolver.FAILED_TO_RESOLVE_DATASET;
        }
        return Iterables.getOnlyElement(allMatching);
    }

    @Nullable
    public AggregateMetric.NeedsSubstitution resolveMetricAlias(final JQLParser.IdentifierContext ctx) {
        final String alias = fieldResolver.canonicalizeMetricAlias(Identifiers.extractIdentifier(ctx));
        if (alias == null) {
            return null;
        }
        final AggregateMetric.NeedsSubstitution needsSubstitution = new AggregateMetric.NeedsSubstitution(alias);
        needsSubstitution.copyPosition(ctx);
        return needsSubstitution;
    }

    private FieldSet resolve(final String typedField, final boolean restricted, @Nullable final ParserRuleContext syntacticCtx) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (final String dataset : scope) {
            if (fieldResolver.canonicalizeMetricAlias(typedField) != null) {
                fieldResolver.addError(new IqlKnownException.UnknownFieldException("Metric alias cannot be used as a field: \"" + typedField + "\""));
                builder.put(dataset, FieldResolver.FAILED_TO_RESOLVE_FIELD);
                continue;
            }
            if (FieldResolver.FAILED_TO_RESOLVE_DATASET.equals(dataset)) {
                fieldResolver.addError(new IqlKnownException.UnknownFieldException("Cannot resolve field for unresolved dataset: \"" + typedField + "\""));
                builder.put(dataset, FieldResolver.FAILED_TO_RESOLVE_FIELD);
                continue;
            }
            try {
                final String resolved = fieldResolver.datasets.get(dataset).resolveFieldName(typedField);
                builder.put(dataset, resolved);
            } catch (IqlKnownException e) {
                fieldResolver.addError(e);
                builder.put(dataset, FieldResolver.FAILED_TO_RESOLVE_FIELD);
            }
        }
        final Map<String, String> datasetToField = builder.build();
        final FieldType type = fieldType(datasetToField.keySet(), datasetToField::get);
        return FieldSet.create(datasetToField, restricted, syntacticCtx, type == FieldType.Integer);
    }

    private FieldSet resolve(final JQLParser.IdentifierContext ctx, final boolean restricted, @Nullable final ParserRuleContext syntacticCtx) {
        final String typedField = Identifiers.extractIdentifier(ctx);
        return resolve(typedField, restricted, syntacticCtx);
    }

    public FieldSet resolve(final JQLParser.IdentifierContext identifierContext) {
        return resolve(identifierContext, false, identifierContext);
    }

    public FieldSet resolve(final JQLParser.SinglyScopedFieldContext ctx) {
        final ScopedFieldResolver resolver = forScope(ctx);
        return resolver.resolve(ctx.field, resolver != this, ctx);
    }

    public FieldSet resolve(final JQLParser.ScopedFieldContext ctx) {
        final ScopedFieldResolver resolver = forScope(ctx);
        return resolver.resolve(ctx.field, resolver != this, ctx);
    }

    public FieldType fieldType(final FieldSet field) {
        return fieldType(field.datasets(), field::datasetFieldName);
    }

    private FieldType fieldType(final Set<String> datasets, final Function<String, String> datasetToFieldName) {
        boolean anyString = false;
        boolean anyInt = false;
        for (final String dataset : datasets) {
            if (FieldResolver.FAILED_TO_RESOLVE_DATASET.equals(dataset)) {
                continue;
            }
            final DatasetMetadata metadata = fieldResolver.datasets.get(dataset).datasetMetadata;
            final String datasetFieldName = datasetToFieldName.apply(dataset);
            if (FieldResolver.FAILED_TO_RESOLVE_FIELD.equals(datasetFieldName)) {
                continue;
            }
            for (final FieldMetadata fieldMetadata : metadata.getFieldAllTypes(datasetFieldName)) {
                switch (fieldMetadata.getType()) {
                    case String:
                        anyString = true;
                        break;
                    case Integer:
                        anyInt = true;
                        break;
                }
            }
        }

        if (anyInt && anyString) {
            return fieldResolver.conflictedFieldType;
        }

        if (anyInt) {
            return FieldType.Integer;
        }

        if (anyString) {
            return FieldType.String;
        }

        // Should only occur if we didn't resolve any fields with the given name,
        // and we don't want to throw an exception so that we can get our
        // parsing validation errors that indicate the source of failure.
        // Let's pretend we're a String!

        log.warn("FieldSet did not contain any valid dataset fields, so defaulting to String type");

        return FieldType.String;
    }

    public interface MetricResolverCallback<T> {
        /**
         * Found 1 or more plain field.
         * Does NOT imply that there is not additionally a metric found for at least 1 dataset.
         */
        T plainFields(final FieldSet fieldSet);

        /**
         * Found at least one dimensions metric.
         * returns null if operation does not work with dimension metric
         */
        @Nullable
        T metric(final DocMetric metric);

        default MetricResolverCallback<T> map(final Function<? super T, ? extends T> function) {
            final MetricResolverCallback<T> original = this;
            return new MetricResolverCallback<T>() {
                @Override
                public T plainFields(final FieldSet fieldSet) {
                    return function.apply(original.plainFields(fieldSet));
                }

                @Override
                @Nullable
                public T metric(final DocMetric metric) {
                    final T originalResult = original.metric(metric);
                    if (originalResult == null) {
                        return null;
                    }
                    return function.apply(originalResult);
                }
            };
        }
    }

    public DocMetric resolveDocMetric(final JQLParser.SinglyScopedFieldContext ctx, final MetricResolverCallback<DocMetric> callback, final Query.Context queryContext) {
        final ScopedFieldResolver resolver = forScope(ctx);
        final DocMetric metric = resolver.resolveDocMetric(ctx.field, callback, queryContext);
        if (resolver != this) {
            return new DocMetric.Qualified(Iterables.getOnlyElement(resolver.scope), metric);
        }
        return metric;
    }

    public DocMetric resolveDocMetric(final JQLParser.IdentifierContext ctx, final MetricResolverCallback<DocMetric> callback, final Query.Context queryContext) {
        return resolvePotentialDocMetric(ctx, ctx, callback, queryContext);
    }

    public DocFilter resolveDocFilter(final JQLParser.SinglyScopedFieldContext ctx, final MetricResolverCallback<DocFilter> callback, final Query.Context queryContext) {
        final ScopedFieldResolver resolver = forScope(ctx);
        final DocFilter filter = resolver.resolveDocFilter(ctx.field, callback, queryContext);
        if (resolver != this) {
            return new DocFilter.Qualified(new ArrayList<>(resolver.scope), filter);
        }
        return filter;
    }

    public DocFilter resolveDocFilter(final JQLParser.IdentifierContext ctx, final MetricResolverCallback<DocFilter> callback, final Query.Context queryContext) {
        return resolvePotentialDocMetric(ctx, ctx, callback, queryContext);
    }

    private <T extends AbstractPositional> T resolvePotentialDocMetric(final JQLParser.IdentifierContext ctx, final ParserRuleContext syntacticCtx, final MetricResolverCallback<T> callback, final Query.Context queryContext) {
        final Map<String, DocMetric> datasetToMetric = new HashMap<>();
        boolean foundDimensionMetric = false;

        final String identifier = Identifiers.extractIdentifier(ctx);

        for (final String dataset : scope) {
            final ScopedFieldResolver scopedResolver = forScope(Collections.singleton(dataset));
            final MetricMetadata metricMetadata = scopedResolver.lookupDimensionMetric(dataset, identifier);
            if (metricMetadata != null) {
                final DocMetric dimensionMetric = SubstituteDimension.getDocMetricOrThrow(metricMetadata, fieldResolver.datasetsMetadata, scopedResolver, queryContext);
                datasetToMetric.put(dataset, dimensionMetric);
                foundDimensionMetric = true;
            } else {
                datasetToMetric.put(dataset, new DocMetric.Field(scopedResolver.resolve(ctx)));
            }

        }

        final T result;
        if (foundDimensionMetric) {
            if (datasetToMetric.size() == 1) {
                result = callback.metric(Iterables.getOnlyElement(datasetToMetric.values()));
            } else {
                result = callback.metric(new DocMetric.PerDatasetDocMetric(ImmutableMap.copyOf(datasetToMetric)));
            }
            if (result == null) {
                throw new IqlKnownException.ParseErrorException("Operation cannot be applied to dimension metric '" + identifier + "'");
            }
        } else {
            result = callback.plainFields(resolve(ctx));
        }

        result.copyPosition(syntacticCtx);

        return result;
    }

    public AggregateMetric resolveAggregateMetric(final JQLParser.SinglyScopedFieldContext ctx, final Query.Context queryContext) {
        final ScopedFieldResolver resolver = forScope(ctx);
        final AggregateMetric filter = resolver.resolveAggregateMetric(ctx.field, queryContext);
        if (resolver != this) {
            return new AggregateMetric.Qualified(new ArrayList<>(resolver.scope), filter);
        }
        return filter;
    }

    public AggregateMetric resolveAggregateMetric(final JQLParser.IdentifierContext ctx, final Query.Context queryContext) {
        return resolvePotentialAggregateMetric(ctx, ctx, queryContext);
    }

    private AggregateMetric resolvePotentialAggregateMetric(final JQLParser.IdentifierContext ctx, final ParserRuleContext syntacticCtx, final Query.Context queryContext) {
        boolean foundDimensionMetric = false;

        final String identifier = Identifiers.extractIdentifier(ctx);

        final List<AggregateMetric> possiblyQualifiedMetrics = new ArrayList<>(scope.size());
        for (final String dataset : scope) {
            final ScopedFieldResolver scopedResolver = forScope(Collections.singleton(dataset));
            final MetricMetadata metricMetadata = scopedResolver.lookupDimensionMetric(dataset, identifier);
            final AggregateMetric metric;
            if (metricMetadata != null) {
                metric = SubstituteDimension.getAggregateMetric(metricMetadata, fieldResolver.datasetsMetadata, scopedResolver, queryContext);
                foundDimensionMetric = true;
            } else {
                metric = new AggregateMetric.DocStats(new DocMetric.Field(scopedResolver.resolve(ctx)));
            }

            final AggregateMetric possiblyQualifiedMetric;
            if (scope.size() > 1) {
                possiblyQualifiedMetric = new AggregateMetric.Qualified(Collections.singletonList(dataset), metric);
            } else {
                possiblyQualifiedMetric = metric;
            }

            possiblyQualifiedMetrics.add(possiblyQualifiedMetric);
        }

        final AggregateMetric result;
        if (foundDimensionMetric) {
            result = AggregateMetric.Add.create(possiblyQualifiedMetrics);
        } else {
            result = new AggregateMetric.DocStats(new DocMetric.Field(resolve(ctx)));
        }

        result.copyPosition(syntacticCtx);

        return result;
    }

    @Nullable
    private MetricMetadata lookupDimensionMetric(final String dataset, final String typedField) {
        final DatasetsMetadata datasetsMetadata = fieldResolver.datasetsMetadata;
        final ResolvedDataset resolvedDataset = fieldResolver.datasets.get(dataset);

        if (resolvedDataset == null) {
            return null;
        }

        final String imhotepDataset = resolvedDataset.imhotepName;

        if (datasetsMetadata.getMetadata(imhotepDataset).isPresent()) {
            final DatasetMetadata metadata = datasetsMetadata.getMetadata(imhotepDataset).get();
            final MetricMetadata metricMetadata = metadata.resolveMetric(typedField);
            if ((metricMetadata != null) && !metricMetadata.isAlias() && (metricMetadata.getExpression() != null)) {
                return metricMetadata;
            }
        }

        return null;
    }

    public FieldSet resolveContextless(final String field) {
        return resolve(field, false, null);
    }

    private ScopedFieldResolver forScope(final JQLParser.ScopedFieldContext ctx) {
        if (ctx.oneScope != null) {
            return forScope(Collections.singleton(resolveDataset(ctx.oneScope).unwrap()));
        } else if (!ctx.manyScope.isEmpty()) {
            final Set<String> scope = ctx.manyScope
                    .stream()
                    .map(this::resolveDataset)
                    .map(Positioned::unwrap)
                    .collect(Collectors.toSet());
            return forScope(scope);
        } else {
            return this;
        }
    }

    private ScopedFieldResolver forScope(final JQLParser.SinglyScopedFieldContext ctx) {
        if (ctx.oneScope != null) {
            return forScope(Collections.singleton(resolveDataset(ctx.oneScope).unwrap()));
        } else {
            return this;
        }
    }

    public ScopedFieldResolver forScope(final Set<String> newScope) {
        Preconditions.checkArgument(scope.containsAll(Sets.difference(newScope, FieldResolver.FAILED_TO_RESOLVE_DATASET_SET)), "Inner scope must be a subset of outer scope (new: %s) (old: %s)", newScope, scope);
        if (newScope.equals(scope)) {
            return this;
        }
        return fieldResolver.forScope(newScope);
    }

    public static final MetricResolverCallback<DocMetric> PLAIN_DOC_METRIC_CALLBACK = new MetricResolverCallback<DocMetric>() {
        @Override
        public DocMetric plainFields(final FieldSet fieldSet) {
            return new DocMetric.Field(fieldSet);
        }

        @Override
        public DocMetric metric(final DocMetric metric) {
            return metric;
        }
    };

    public static class HasTermCallback implements MetricResolverCallback<DocMetric> {
        private final Term term;

        public HasTermCallback(final Term term) {
            this.term = term;
        }

        @Override
        public DocMetric plainFields(final FieldSet fieldSet) {
            return DocMetrics.hasTermMetricOrThrow(fieldSet, term);
        }

        @Override
        @Nullable
        public DocMetric metric(final DocMetric metric) {
            if (!term.isIntTerm()) {
                return null;
            }
            return DocMetric.MetricEqual.create(metric, new DocMetric.Constant(term.getIntTerm()));
        }
    }

    public static class FieldIsCallback implements MetricResolverCallback<DocFilter> {
        private final Term term;

        public FieldIsCallback(final Term term) {
            this.term = term;
        }

        @Override
        public DocFilter plainFields(final FieldSet fieldSet) {
            return DocFilter.FieldIs.create(fieldSet, term);
        }

        @Override
        @Nullable
        public DocFilter metric(final DocMetric metric) {
            if (!term.isIntTerm()) {
                return null;
            }
            return new DocFilter.MetricEqual(metric, new DocMetric.Constant(term.getIntTerm()));
        }
    }

    public static class FieldIsntCallback implements MetricResolverCallback<DocFilter> {
        private final Term term;

        public FieldIsntCallback(final Term term) {
            this.term = term;
        }

        @Override
        public DocFilter plainFields(final FieldSet fieldSet) {
            return DocFilter.FieldIsnt.create(fieldSet, term);
        }

        @Override
        @Nullable
        public DocFilter metric(final DocMetric metric) {
            if (!term.isIntTerm()) {
                return null;
            }
            return new DocFilter.MetricNotEqual(metric, new DocMetric.Constant(term.getIntTerm()));
        }
    }
}
