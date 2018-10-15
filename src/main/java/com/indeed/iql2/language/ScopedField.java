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

package com.indeed.iql2.language;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.iql2.language.JQLParser;

import java.util.Collections;
import java.util.List;

import static com.indeed.iql2.language.Identifiers.parseIdentifier;

public class ScopedField {
    public final List<String> scope;
    public final Positioned<String> field;

    private ScopedField(List<String> scope, Positioned<String> field) {
        this.scope = ImmutableList.copyOf(scope);
        this.field = field;
    }

    public static ScopedField parseFrom(JQLParser.ScopedFieldContext ctx) {
        final List<String> scope;
        if (ctx.manyScope.isEmpty()) {
            scope = ctx.oneScope != null ? Collections.singletonList(parseIdentifier(ctx.oneScope).unwrap()) : Collections.<String>emptyList();
        } else {
            scope = Lists.newArrayListWithCapacity(ctx.manyScope.size());
            for (final JQLParser.IdentifierContext identifier : ctx.manyScope) {
                scope.add(parseIdentifier(identifier).unwrap());
            }
        }
        return new ScopedField(scope, parseIdentifier(ctx.field));
    }

    public static ScopedField parseFrom(JQLParser.SinglyScopedFieldContext singlyScopedFieldContext) {
        final List<String> scope;
        if (singlyScopedFieldContext.oneScope != null) {
            scope = Collections.singletonList(parseIdentifier(singlyScopedFieldContext.oneScope).unwrap());
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


    @Override
    public String toString() {
        return "ScopedField{" +
                "scope=" + scope +
                ", field=" + field +
                '}';
    }
}
