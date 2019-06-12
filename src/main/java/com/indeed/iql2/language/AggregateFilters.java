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
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class AggregateFilters {
    private AggregateFilters() {
    }

    public static AggregateFilter aggregateInHelper(final Iterator<Term> terms, final boolean negate) {
        final List<AggregateFilter> termFilters = new ArrayList<>();
        while (terms.hasNext()) {
            termFilters.add(new AggregateFilter.TermIs(terms.next()));
        }
        AggregateFilter filter = AggregateFilter.Or.create(termFilters);
        if (negate) {
            filter = new AggregateFilter.Not(filter);
        }
        return filter;
    }

    public static AggregateFilter parseAggregateFilter(
            final JQLParser.AggregateFilterContext aggregateFilterContext,
            final Query.Context context) {
        if (aggregateFilterContext.jqlAggregateFilter() != null) {
            return parseJQLAggregateFilter(aggregateFilterContext.jqlAggregateFilter(), context);
        }
        throw new UnsupportedOperationException("Non-JQL aggregate filters don't exist. What did you do?!?!?!");
    }

    public static AggregateFilter parseJQLAggregateFilter(
            final JQLParser.JqlAggregateFilterContext aggregateFilterContext,
            final Query.Context context) {
        final AggregateFilter[] ref = new AggregateFilter[1];
        final ScopedFieldResolver fieldResolver = context.fieldResolver;

        aggregateFilterContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateFalse(JQLParser.AggregateFalseContext ctx) {
                accept(new AggregateFilter.Never());
            }

            public void enterAggregateTermIs(JQLParser.AggregateTermIsContext ctx) {
                accept(new AggregateFilter.TermIs(Term.parseJqlTerm(ctx.jqlTermVal(), context.warn)));
            }

            public void enterAggregateTermRegex(JQLParser.AggregateTermRegexContext ctx) {
                accept(new AggregateFilter.TermRegex(Term.term(ParserCommon.unquote(ctx.STRING_LITERAL().getText(), context.warn))));
            }

            public void enterAggregateRegex(JQLParser.AggregateRegexContext ctx) {
                checkAggregateRegex(ctx.field);
                accept(new AggregateFilter.Regex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText(), context.warn)));
            }

            public void enterAggregateNotRegex(JQLParser.AggregateNotRegexContext ctx) {
                checkAggregateRegex(ctx.field);
                accept(new AggregateFilter.Not(new AggregateFilter.Regex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText(), context.warn))));
            }

            public void enterAggregateTrue(JQLParser.AggregateTrueContext ctx) {
                accept(new AggregateFilter.Always());
            }

            public void enterAggregateFilterParens(JQLParser.AggregateFilterParensContext ctx) {
                accept(parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
            }

            public void enterAggregateAnd(JQLParser.AggregateAndContext ctx) {
                final AggregateFilter left = parseJQLAggregateFilter(ctx.jqlAggregateFilter(0), context);
                final AggregateFilter right = parseJQLAggregateFilter(ctx.jqlAggregateFilter(1), context);
                accept(AggregateFilter.And.create(left, right));
            }

            public void enterAggregateMetricInequality(JQLParser.AggregateMetricInequalityContext ctx) {
                final String operation = ctx.op.getText();
                final AggregateMetric arg1 = AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), context);
                final AggregateMetric arg2 = AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), context);
                final AggregateFilter result;
                switch (operation) {
                    case "=": {
                        result = new AggregateFilter.MetricIs(arg1, arg2);
                        break;
                    }
                    case "!=": {
                        result = new AggregateFilter.MetricIsnt(arg1, arg2);
                        break;
                    }
                    case "<": {
                        result = new AggregateFilter.Lt(arg1, arg2);
                        break;
                    }
                    case "<=": {
                        result = new AggregateFilter.Lte(arg1, arg2);
                        break;
                    }
                    case ">": {
                        result = new AggregateFilter.Gt(arg1, arg2);
                        break;
                    }
                    case ">=": {
                        result = new AggregateFilter.Gte(arg1, arg2);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unhandled inequality operation: [" + operation + "]");
                }
                accept(result);
            }

            public void enterAggregateNot(JQLParser.AggregateNotContext ctx) {
                accept(new AggregateFilter.Not(parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context)));
            }

            public void enterAggregateOr(final JQLParser.AggregateOrContext ctx) {
                final AggregateFilter left = parseJQLAggregateFilter(ctx.jqlAggregateFilter(0), context);
                final AggregateFilter right = parseJQLAggregateFilter(ctx.jqlAggregateFilter(1), context);
                accept(AggregateFilter.Or.create(ImmutableList.of(left, right)));
            }

            private void checkAggregateRegex(final JQLParser.IdentifierContext fieldContext) {
                context.warn.accept("Aggregate regex (e.g. HAVING fieldName =~ \"regex\") is deprecated. Please use TERM() =~ \"regex\" or NOT(TERM() =~ \"regex\") instead.");
                final FieldSet resolvedField = fieldResolver.resolve(fieldContext);
                if (context.aggregateContexts.isEmpty()) {
                    throw new IllegalStateException("AggregateRegex is applied without aggregate context");
                }
                context.aggregateContexts.top().accept(new Query.AggregateContext.Matcher() {
                    @Override
                    public void matchField(final FieldSet field) {
                        if (!resolvedField.equals(field)) {
                            final String regexFieldString = fieldContext.getText();
                            final String groupByFieldString = Optional.ofNullable(field.getRawInput()).orElseGet(field::toString);
                            throw new IllegalArgumentException(
                                    "Can't apply aggregate regex on field " + regexFieldString + " in aggregate context " + groupByFieldString + ".\nDid you mean [" + regexFieldString + " =~ \"...\"] > 0?");
                        }
                    }

                    @Override
                    public void matchMetric() {
                        throw new UnsupportedOperationException("AggregateFilter.Regex should be applied to a field regroup");
                    }

                    @Override
                    public void matchSession() {
                        throw new UnsupportedOperationException("AggregateFilter.Regex shouldn be applied to a field regroup");
                    }
                });
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate filter: [" + aggregateFilterContext.getText() + "]");
        }

        ref[0].copyPosition(aggregateFilterContext);

        return ref[0];
    }
}
