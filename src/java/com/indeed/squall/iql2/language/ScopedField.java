package com.indeed.squall.iql2.language;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

public class ScopedField {
    public final List<String> scope;
    public final String field;

    private ScopedField(List<String> scope, String field) {
        this.scope = ImmutableList.copyOf(scope);
        this.field = field;
    }

    public static ScopedField parseFrom(JQLParser.ScopedFieldContext ctx) {
        final List<String> scope;
        if (ctx.manyScope.isEmpty()) {
            scope = ctx.oneScope != null ? Collections.singletonList(parseIdentifier(ctx.oneScope)) : Collections.<String>emptyList();
        } else {
            scope = Lists.newArrayListWithCapacity(ctx.manyScope.size());
            for (final JQLParser.IdentifierContext identifier : ctx.manyScope) {
                scope.add(parseIdentifier(identifier));
            }
        }
        return new ScopedField(scope, parseIdentifier(ctx.field));
    }

    public static ScopedField parseFrom(JQLParser.SinglyScopedFieldContext singlyScopedFieldContext) {
        final List<String> scope;
        if (singlyScopedFieldContext.oneScope != null) {
            scope = Collections.singletonList(parseIdentifier(singlyScopedFieldContext.oneScope));
        } else {
            scope = Collections.emptyList();
        }
        return new ScopedField(scope, parseIdentifier(singlyScopedFieldContext.field));
    }

    public AggregateMetric wrap(AggregateMetric metric) {
        if (scope.isEmpty()) {
            return metric;
        } else {
            return new AggregateMetric.Qualified(scope, metric);
        }
    }

    public DocFilter wrap(DocFilter docFilter) {
        if (scope.isEmpty()) {
            return docFilter;
        } else {
            return new DocFilter.Qualified(scope, docFilter);
        }
    }

    public DocMetric wrap(DocMetric docMetric) {
        if (scope.isEmpty()) {
            return docMetric;
        } else if (scope.size() == 1) {
            return new DocMetric.Qualified(scope.get(0), docMetric);
        } else {
            throw new IllegalArgumentException("Too large scope for a DocMetric!: " + this.scope);
        }
    }
}
