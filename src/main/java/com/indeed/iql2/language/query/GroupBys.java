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

package com.indeed.iql2.language.query;

import com.google.common.collect.ImmutableSet;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateFilters;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.AggregateMetrics;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocFilters;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetrics;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.JQLBaseListener;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.ParserCommon;
import com.indeed.iql2.language.SortOrder;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.commands.TopK;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.util.core.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class GroupBys {
    private GroupBys() {
    }

    private static final ImmutableSet<String> VALID_ORDERINGS = ImmutableSet.of("bottom", "descending", "desc");

    public static Pair<List<GroupByEntry>, Query.Context> parseGroupBys(
            final JQLParser.GroupByContentsContext groupByContentsContext,
            final Query.Context context) {
        final List<JQLParser.GroupByEntryContext> elements = groupByContentsContext.groupByEntry();
        final List<GroupByEntry> result = new ArrayList<>(elements.size());
        Query.Context newContext = context;
        for (final JQLParser.GroupByEntryContext element : elements) {
            final Pair<GroupByEntry, Query.Context> groupByEntry = parseGroupByEntry(element, newContext);
            result.add(groupByEntry.getFirst());
            newContext = groupByEntry.getSecond();
        }
        return Pair.of(result, newContext);
    }

    public static Pair<GroupByEntry, Query.Context> parseGroupByEntry(
            final JQLParser.GroupByEntryContext ctx,
            final Query.Context context
    ) {
        final Pair<GroupBy, Query.Context> groupBy = parseGroupByAndGetAggregatedContext(ctx.groupByElement(), context);
        final Query.Context aggregatedContext = groupBy.getSecond();
        Optional<AggregateFilter> aggregateFilter = Optional.empty();
        Optional<String> alias = Optional.empty();
        if (ctx.filter != null) {
            aggregateFilter = Optional.of(AggregateFilters.parseAggregateFilter(ctx.filter, aggregatedContext));
        }
        if (ctx.alias != null) {
            alias = Optional.of(ctx.alias.getText());
        }
        return Pair.of(new GroupByEntry(groupBy.getFirst(), aggregateFilter, alias), aggregatedContext);
    }

    public static Pair<GroupBy, Query.Context> parseGroupByAndGetAggregatedContext(
            final JQLParser.GroupByElementContext groupByElementContext,
            final Query.Context context) {
        final AtomicReference<Pair<GroupBy, Query.Context>> ref = new AtomicReference<>();
        final ScopedFieldResolver fieldResolver = context.fieldResolver;

        groupByElementContext.enterRule(new JQLBaseListener() {
            private void accept(final GroupBy value, final Query.Context aggregatedContext) {
                if (ref.get() != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref.set(Pair.of(value, aggregatedContext));
            }

            @Override
            public void enterDayOfWeekGroupBy(JQLParser.DayOfWeekGroupByContext ctx) {
                if (ctx.hasParens == null) {
                    context.warn.accept("DAYOFWEEK is deprecated -- should be DAYOFWEEK().");
                }
                accept(new GroupBy.GroupByDayOfWeek(), context.withMetricAggregate());
            }

            @Override
            public void enterQuantilesGroupBy(JQLParser.QuantilesGroupByContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                accept(new GroupBy.GroupByQuantiles(field, Integer.parseInt(ctx.NAT().getText())), context.withMetricAggregate());
            }

            @Override
            public void enterTopTermsGroupBy(JQLParser.TopTermsGroupByContext ctx) {
                final JQLParser.TopTermsGroupByElemContext ctx2 = ctx.topTermsGroupByElem();
                final FieldSet field = fieldResolver.resolve(ctx2.field);
                final Query.Context aggregatedContext = context.withFieldAggregate(field);
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.empty();
                }
                Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    metric = Optional.of(AggregateMetrics.parseAggregateMetric(ctx2.metric, aggregatedContext));
                } else {
                    metric = Optional.empty();
                }
                SortOrder sortOrder = SortOrder.ASCENDING;
                if (ctx2.order != null) {
                    if (VALID_ORDERINGS.contains(ctx2.order.getText().toLowerCase())) {
                        metric = Optional.<AggregateMetric>of(new AggregateMetric.Negate(metric.get()));
                    }
                }
                final Optional<TopK> topK = (metric.isPresent() || limit.isPresent()) ? Optional.of(new TopK(limit, metric, sortOrder)) : Optional.empty();
                accept(new GroupBy.GroupByField(field, Optional.empty(), topK, false), aggregatedContext);
            }

            @Override
            public void enterGroupByFieldIn(final JQLParser.GroupByFieldInContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final Query.Context aggregatedContext = context.withFieldAggregate(field);
                if (ctx.not != null) {
                    final Iterator<Term> terms = ctx.terms.stream().map(Term::parseTerm).iterator();
                    final AggregateFilter filter = AggregateFilters.aggregateInHelper(terms, true);
                    accept(new GroupBy.GroupByField(field, Optional.of(filter), Optional.empty(), ctx.withDefault != null), aggregatedContext);
                } else {
                    final ImmutableSet<Term> terms = ImmutableSet.copyOf(ctx.terms.stream().map(Term::parseTerm).iterator());
                    final boolean withDefault = ctx.withDefault != null;
                    accept(new GroupBy.GroupByFieldIn(field, terms, withDefault, ctx.useLegacy), aggregatedContext);
                }
            }

            @Override
            public void enterGroupByFieldInQuery(final JQLParser.GroupByFieldInQueryContext ctx) {
                final JQLParser.QueryNoSelectContext queryCtx = ctx.queryNoSelect();
                final Query query = Query.parseSubquery(queryCtx, context.withoutAggregate());
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final Query.Context aggregatedContext = context.withFieldAggregate(field);
                accept(new GroupBy.GroupByFieldInQuery(field, query, ctx.not != null, ctx.withDefault != null, context.datasetsMetadata), aggregatedContext);
            }

            @Override
            public void enterMetricGroupBy(final JQLParser.MetricGroupByContext ctx) {
                final DocMetric metric;
                final long min;
                final long max;
                final long interval;
                final boolean excludeGutters;
                final boolean withDefault;
                final Query.Context aggregatedContext = context.withMetricAggregate();
                if (ctx.groupByMetric() != null) {
                    final JQLParser.GroupByMetricContext ctx2 = ctx.groupByMetric();
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), context);
                    min = Long.parseLong(ctx2.min.getText());
                    max = Long.parseLong(ctx2.max.getText());
                    interval = Long.parseLong(ctx2.interval.getText());
                    if (ctx2.gutterID != null) {
                        final String text = ctx2.gutterID.getText();
                        excludeGutters = "true".equalsIgnoreCase(text);
                    } else if (ctx2.gutterNumber != null) {
                        excludeGutters = "1".equals(ctx2.gutterNumber.getText());
                    } else {
                        excludeGutters = false;
                    }
                    withDefault = ctx2.withDefault != null;
                } else {
                    throw new UnsupportedOperationException("Oh no! Someone changed the parser but not the consumer!");
                }
                if (excludeGutters && withDefault) {
                    throw new IllegalArgumentException("Can't use both excludeGutters and withDefault explicitly! Just use with default.");
                }
                accept(new GroupBy.GroupByMetric(metric, min, max, interval, excludeGutters || withDefault, withDefault), aggregatedContext);
            }

            @Override
            public void enterTimeGroupBy(final JQLParser.TimeGroupByContext ctx) {
                final boolean isRelative = ctx.groupByTime().isRelative != null;
                final Query.Context aggregatedContext = context.withMetricAggregate();

                if (ctx.groupByTime().timeBucket() == null) {
                    accept(new GroupBy.GroupByInferredTime(isRelative), aggregatedContext);
                    return;
                }

                final Optional<FieldSet> timeField;
                if (ctx.groupByTime().timeField != null) {
                    timeField = Optional.of(fieldResolver.resolve(ctx.groupByTime().timeField));
                } else {
                    timeField = Optional.empty();
                }

                final Optional<String> timeFormat;
                if (ctx.groupByTime().timeFormat != null) {
                    final String format = ctx.groupByTime().timeFormat.getText();
                    if (format.startsWith("\'") && format.endsWith("\'")) {
                        timeFormat = Optional.of(ParserCommon.unquote(format));
                    } else if (format.startsWith("\"") && format.endsWith("\"")) {
                        timeFormat = Optional.of(ParserCommon.unquote(format));
                    } else {
                        timeFormat = Optional.of(format);
                    }
                } else {
                    timeFormat = Optional.empty();
                }

                final List<Pair<Integer, TimeUnit>> pairs = TimePeriods.parseTimeBuckets(ctx.groupByTime().timeBucket(), ctx.useLegacy);
                long millisSum = 0L;
                for (final Pair<Integer, TimeUnit> pair : pairs) {
                    final int coeff = pair.getFirst();
                    final TimeUnit unit = pair.getSecond();

                    if ((unit == TimeUnit.BUCKETS) && (pairs.size() > 1)) {
                        throw new IqlKnownException.ParseErrorException("Can't group by buckets and also other time units in the same time group by");
                    } else if ((unit == TimeUnit.MONTH) && (pairs.size() > 1)) {
                        throw new IqlKnownException.ParseErrorException("Can't group by months and also other time units in the same time group by");
                    } else if ((unit == TimeUnit.MONTH) && (coeff != 1)) {
                        throw new IqlKnownException.ParseErrorException("Month group by must be 1 month for time group-by.");
                    }

                    if (unit == TimeUnit.BUCKETS) {
                        accept(new GroupBy.GroupByTimeBuckets(coeff, timeField, timeFormat, isRelative), aggregatedContext);
                        return;
                    }
                    if (unit == TimeUnit.MONTH) {
                        accept(new GroupBy.GroupByMonth(timeField, timeFormat), aggregatedContext);
                        return;
                    }

                    millisSum += coeff * unit.millis;
                }

                accept(new GroupBy.GroupByTime(millisSum, timeField, timeFormat, isRelative), aggregatedContext);
            }

            @Override
            public void enterFieldGroupBy(final JQLParser.FieldGroupByContext ctx) {
                final JQLParser.GroupByFieldContext ctx2 = ctx.groupByField();
                final FieldSet field = fieldResolver.resolve(ctx2.field);
                final Query.Context aggregatedContext = context.withFieldAggregate(field);
                final SortOrder sortOrder = ((ctx2.order != null) && ctx2.order.getText().equalsIgnoreCase("bottom")) ? SortOrder.DESCENDING : SortOrder.ASCENDING;
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.empty();
                }
                final Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    final AggregateMetric theMetric = AggregateMetrics.parseAggregateMetric(ctx2.metric, aggregatedContext);
                    metric = Optional.of(theMetric);
                } else {
                    metric = Optional.empty();
                }
                final Optional<AggregateFilter> filter;
                if (ctx2.filter != null) {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx2.filter, aggregatedContext));
                } else {
                    filter = Optional.empty();
                }
                final boolean withDefault = ctx2.withDefault != null;
                final Optional<TopK> topK = (metric.isPresent() || limit.isPresent()) ? Optional.of(new TopK(limit, metric, sortOrder)) : Optional.empty();
                accept(new GroupBy.GroupByField(field, filter, topK, withDefault), aggregatedContext);
            }

            @Override
            public void enterDatasetGroupBy(JQLParser.DatasetGroupByContext ctx) {
                accept(new GroupBy.GroupBySessionName(), context.withSessionAggregate());
            }

            @Override
            public void enterPredicateGroupBy(JQLParser.PredicateGroupByContext ctx) {
                final DocFilter filter = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), context);
                accept(new GroupBy.GroupByPredicate(filter), context.withMetricAggregate());
            }

            @Override
            public void enterRandomGroupBy(JQLParser.RandomGroupByContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final int k = Integer.parseInt(ctx.k.getText());
                final String salt;
                if (ctx.salt == null) {
                    salt = "DEFAULT SALT";
                } else {
                    salt = ParserCommon.unquote(ctx.salt.getText());
                }
                accept(new GroupBy.GroupByRandom(field, k, salt), context.withMetricAggregate());
            }

            @Override
            public void enterRandomMetricGroupBy(final JQLParser.RandomMetricGroupByContext ctx) {
                final DocMetric metric = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), context);
                final int k = Integer.parseInt(ctx.k.getText());
                final String salt;
                if (ctx.salt == null) {
                    salt = "DEFAULT SALT";
                } else {
                    salt = ParserCommon.unquote(ctx.salt.getText());
                }
                accept(new GroupBy.GroupByRandomMetric(metric, k, salt), context.withMetricAggregate());
            }
        });

        if (ref.get() == null) {
            throw new UnsupportedOperationException("Failed to handle group by: " + groupByElementContext.getText());
        }

        ref.get().getFirst().copyPosition(groupByElementContext);

        return ref.get();
    }
}
