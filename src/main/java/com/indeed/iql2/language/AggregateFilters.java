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

import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;

public class AggregateFilters {
    public static AggregateFilter aggregateInHelper(final Iterable<Term> terms, final boolean negate) {
        AggregateFilter filter = null;
        for (final Term term : terms) {
            if (filter == null) {
                filter = new AggregateFilter.TermIs(term);
            } else {
                filter = new AggregateFilter.Or(new AggregateFilter.TermIs(term), filter);
            }
        }
        if (filter == null) {
            // TODO (optional): Make this new Always() and don't negate if (ctx.not != null).
            // TODO cont:       Alternatively, add optimization pass for Not(Always) and Not(Never).
            filter = new AggregateFilter.Never();
        }
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

            public void enterAggregateRegex(JQLParser.AggregateRegexContext ctx) {
                accept(new AggregateFilter.Regex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            public void enterAggregateFalse(JQLParser.AggregateFalseContext ctx) {
                accept(new AggregateFilter.Never());
            }

            public void enterAggregateTermIs(JQLParser.AggregateTermIsContext ctx) {
                accept(new AggregateFilter.TermIs(Term.parseJqlTerm(ctx.jqlTermVal())));
            }

            public void enterAggregateTermRegex(JQLParser.AggregateTermRegexContext ctx) {
                accept(new AggregateFilter.TermRegex(Term.term(ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            public void enterAggregateNotRegex(JQLParser.AggregateNotRegexContext ctx) {
                accept(new AggregateFilter.Not(new AggregateFilter.Regex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            public void enterAggregateTrue(JQLParser.AggregateTrueContext ctx) {
                accept(new AggregateFilter.Always());
            }

            public void enterAggregateFilterParens(JQLParser.AggregateFilterParensContext ctx) {
                accept(parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
            }

            public void enterAggregateAnd(JQLParser.AggregateAndContext ctx) {
                accept(new AggregateFilter.And(
                        parseJQLAggregateFilter(ctx.jqlAggregateFilter(0), context),
                        parseJQLAggregateFilter(ctx.jqlAggregateFilter(1), context)));
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

            public void enterAggregateOr(JQLParser.AggregateOrContext ctx) {
                accept(new AggregateFilter.Or(
                        parseJQLAggregateFilter(ctx.jqlAggregateFilter(0), context),
                        parseJQLAggregateFilter(ctx.jqlAggregateFilter(1), context)));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate filter: [" + aggregateFilterContext.getText() + "]");
        }

        ref[0].copyPosition(aggregateFilterContext);

        return ref[0];
    }
}
