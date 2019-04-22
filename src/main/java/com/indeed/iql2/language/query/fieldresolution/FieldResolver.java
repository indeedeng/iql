package com.indeed.iql2.language.query.fieldresolution;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql2.language.Identifiers;
import com.indeed.iql2.language.JQLBaseVisitor;
import com.indeed.iql2.language.JQLParser;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author jwolfe
 */
public class FieldResolver {
    // Value returned for fields that failed to resolve.
    // An exception will also be thrown (either immediate or deferred, depending on mode)
    // if this is returned.
    public static final String FAILED_TO_RESOLVE_FIELD = "FAILED_TO_RESOLVE_F";

    // Value returned for datasets that failed to resolve.
    // An exception will also be thrown (either immediate or deferred, depending on mode)
    // if this is returned.
    public static final String FAILED_TO_RESOLVE_DATASET = "FAILED_TO_RESOLVE_D";

    static final Set<String> FAILED_TO_RESOLVE_DATASET_SET = Collections.singleton(FAILED_TO_RESOLVE_DATASET);

    private final Map<String, Set<String>> metricAliasEquivalenceSets;
    final Map<String, ResolvedDataset> datasets;
    final DatasetsMetadata datasetsMetadata;
    final FieldType conflictedFieldType;

    // In case errorMode is ErrorMode.DEFERRED, this field will contain any
    // exceptions that have been added to the FieldResolver with addError calls.
    // In case errorMode is ErrorMode.IMMEDIATE, this field will always be null.
    // When switching to IMMEDIATE mode, any exceptions present will be thrown immediately.
    @Nullable
    private IqlKnownException error = null;
    private ErrorMode errorMode = ErrorMode.DEFERRED;


    public enum ErrorMode {
        // Immediately throw exceptions when encountered
        IMMEDIATE,
        // Allow exceptions to be retrieved later
        DEFERRED,
    }

    public FieldResolver(
            final Set<String> metricAliases,
            final Map<String, ResolvedDataset> datasets,
            final DatasetsMetadata datasetsMetadata,
            final FieldType conflictedFieldType) {
        metricAliasEquivalenceSets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final String metricAlias : metricAliases) {
            metricAliasEquivalenceSets
                    .computeIfAbsent(metricAlias, ignored -> new HashSet<>())
                    .add(metricAlias);
        }
        this.datasets = datasets;
        this.datasetsMetadata = datasetsMetadata;
        this.conflictedFieldType = conflictedFieldType;
    }

    @Nullable
    public IqlKnownException errors() {
        return error;
    }

    void addError(final IqlKnownException error) {
        switch (errorMode) {
            case IMMEDIATE:
                throw error;
            case DEFERRED:
                if (this.error == null) {
                    this.error = error;
                } else {
                    this.error.addSuppressed(error);
                }
                break;
            default:
                throw new IllegalStateException("Unhandled error mode: " + errorMode);
        }
    }

    /**
     * Will immediately throw any unthrown errors if deferred errors are
     * present and error mode is changed to immediate.
     * Invariant: if errorMode == ErrorMode.IMMEDIATE, errors() is always null.
     * @param errorMode
     */
    public void setErrorMode(final ErrorMode errorMode) {
        try {
            if ((error != null) && (errorMode == ErrorMode.IMMEDIATE)) {
                throw error;
            }
        } finally {
            if (errorMode == ErrorMode.IMMEDIATE) {
                error = null;
            }
            this.errorMode = errorMode;
        }
    }

    public void clearErrors() {
        if (errorMode == ErrorMode.IMMEDIATE) {
            throw new IllegalStateException("Clearing errors in IMMEDIATE mode indicates flawed logic");
        }
        error = null;
    }

    @Nullable
    String canonicalizeMetricAlias(final String typedName) {
        final Set<String> equivalent = metricAliasEquivalenceSets.getOrDefault(typedName, Collections.emptySet());
        // Prefer an exact match if present
        if (equivalent.contains(typedName)) {
            return typedName;
        }
        // No options available
        if (equivalent.isEmpty()) {
            return null;
        }
        // Ensure unambiguous
        if (equivalent.size() > 1) {
            addError(new IqlKnownException.UnknownFieldException("Metric alias is ambiguous: " + equivalent));
            return null;
        }
        // If only one option, use it
        return Iterables.getOnlyElement(equivalent);
    }

    public ScopedFieldResolver universalScope() {
        return forScope(datasets.keySet());
    }

    public ScopedFieldResolver forScope(final Set<String> scope) {
        Preconditions.checkArgument(datasets.keySet().containsAll(Sets.difference(scope, FAILED_TO_RESOLVE_DATASET_SET)));
        return new ScopedFieldResolver(this, scope);
    }

    public static FieldResolver build(
            final ParseTree queryCtx,
            final ParseTree fromCtx,
            final DatasetsMetadata datasetsMetadata,
            final boolean useLegacy) {
        final Map<String, ResolvedDataset> datasets = new HashMap<>();

        // Visitor for finding all relevant datasets and their information
        // (including field aliases)
        final JQLBaseVisitor<Void> fromVisitor = new JQLBaseVisitor<Void>() {
            @Override
            public Void visitDataset(final JQLParser.DatasetContext ctx) {
                processDatasetAliases(ctx.index, ctx.name, ctx.aliases());
                return super.visitDataset(ctx);
            }

            @Override
            public Void visitPartialDataset(final JQLParser.PartialDatasetContext ctx) {
                processDatasetAliases(ctx.index, ctx.name, ctx.aliases());
                return super.visitPartialDataset(ctx);
            }

            @Override
            public Void visitDocFieldInQuery(final JQLParser.DocFieldInQueryContext ctx) {
                // Absolutely do NOT recurse into children.
                // We do not want to see aliases from sub-queries.
                // We do not want to see anything from sub-queries, really.
                return null;
            }

            private void processDatasetAliases(
                    final JQLParser.IdentifierContext index,
                    @Nullable final JQLParser.IdentifierContext name,
                    final JQLParser.AliasesContext aliases
            ) {
                final String typedName = Identifiers.extractIdentifier(index);
                final String imhotepName = datasetsMetadata.resolveDatasetName(typedName);
                final DatasetMetadata metadata = datasetsMetadata
                        .getMetadata(imhotepName)
                        .orElseGet(() -> new DatasetMetadata(imhotepName));
                final String chosenName = (name != null) ? Identifiers.extractIdentifier(name) : imhotepName;

                final Map<String, String> dimensionsAliases = datasetsMetadata.getDatasetToDimensionAliasFields().getOrDefault(imhotepName, Collections.emptyMap());

                final Map<String, String> aliasMapping = new HashMap<>(dimensionsAliases);

                if ((aliases != null) && (aliases.actual != null) && (aliases.virtual != null)) {
                    Preconditions.checkState(
                            aliases.actual.size() == aliases.virtual.size(),
                            "Expected actual and virtual to have same size"
                    );
                    final ResolvedDataset dimensionsOnlyDataset = new ResolvedDataset(chosenName, imhotepName, dimensionsAliases, metadata);
                    for (int i = 0; i < aliases.actual.size(); i++) {
                        String actual = dimensionsOnlyDataset.resolveFieldName(Identifiers.extractIdentifier(aliases.actual.get(i)));
                        actual = dimensionsAliases.getOrDefault(actual, actual);
                        final String virtual = Identifiers.extractIdentifier(aliases.virtual.get(i));
                        aliasMapping.put(virtual, actual);
                    }
                }

                datasets.put(chosenName, new ResolvedDataset(
                        chosenName,
                        imhotepName,
                        aliasMapping,
                        metadata
                ));
            }
        };
        fromVisitor.visit(fromCtx);

        final Set<String> aliasesFound = new HashSet<>();
        // For finding metric aliases
        final JQLBaseVisitor<Void> queryVisitor = new JQLBaseVisitor<Void>() {
            @Override
            public Void visitAggregateNamed(final JQLParser.AggregateNamedContext ctx) {
                aliasesFound.add(Identifiers.extractIdentifier(ctx.name));
                return super.visitAggregateNamed(ctx);
            }
        };
        queryVisitor.visit(queryCtx);

        return new FieldResolver(aliasesFound, datasets, datasetsMetadata, useLegacy ? FieldType.String : FieldType.Integer);
    }

    // Use only in IQL1 -> IQL2 conversion
    // simplified version of build method above
    public static FieldResolver create(
            final String dataset,
            final DatasetsMetadata datasetsMetadata) {
        final Map<String, ResolvedDataset> datasets = new HashMap<>();

        final DatasetMetadata metadata = datasetsMetadata
                .getMetadata(dataset)
                .orElseGet(() -> new DatasetMetadata(dataset));

        final Map<String, String> dimensionsAliases = datasetsMetadata.getDatasetToDimensionAliasFields().getOrDefault(dataset, Collections.emptyMap());

        datasets.put(dataset, new ResolvedDataset(
                dataset,
                dataset,
                dimensionsAliases,
                metadata
        ));

        final Set<String> aliasesFound = new HashSet<>();
        return new FieldResolver(aliasesFound, datasets, datasetsMetadata, FieldType.String);
    }
}
