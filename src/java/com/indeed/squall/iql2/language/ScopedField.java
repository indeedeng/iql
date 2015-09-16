package com.indeed.squall.iql2.language;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

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
            scope = ctx.oneScope != null ? Collections.singletonList(ctx.oneScope.getText().toUpperCase()) : Collections.<String>emptyList();
        } else {
            scope = Lists.newArrayListWithCapacity(ctx.manyScope.size());
            for (final JQLParser.IdentifierContext identifier : ctx.manyScope) {
                scope.add(identifier.getText().toUpperCase());
            }
        }
        return new ScopedField(scope, ctx.field.getText().toUpperCase());
    }

    public static ScopedField parseFrom(JQLParser.SinglyScopedFieldContext singlyScopedFieldContext) {
        final List<String> scope;
        if (singlyScopedFieldContext.oneScope != null) {
            scope = Collections.singletonList(singlyScopedFieldContext.oneScope.getText().toUpperCase());
        } else {
            scope = Collections.emptyList();
        }
        return new ScopedField(scope, singlyScopedFieldContext.field.getText().toUpperCase());
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
