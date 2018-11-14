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

import com.google.common.base.Optional;
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
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.util.core.Pair;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.indeed.iql2.language.Identifiers.parseIdentifier;

public class GroupBys {

    public static final ImmutableSet<String> VALID_ORDERINGS = ImmutableSet.of("bottom", "descending", "desc");

    public static List<GroupByEntry> parseGroupBys(
            final JQLParser.GroupByContentsContext groupByContentsContext,
            final Query.Context context) {
        final List<JQLParser.GroupByEntryContext> elements = groupByContentsContext.groupByEntry();
        final List<GroupByEntry> result = new ArrayList<>(elements.size());
        for (final JQLParser.GroupByEntryContext element : elements) {
            result.add(parseGroupByEntry(element, context));
        }
        return result;
    }

    public static GroupByEntry parseGroupByEntry(
            final JQLParser.GroupByEntryContext ctx,
            final Query.Context context
    ) {
        final GroupBy groupBy = parseGroupBy(ctx.groupByElement(), context);
        Optional<AggregateFilter> aggregateFilter = Optional.absent();
        Optional<String> alias = Optional.absent();
        if (ctx.filter != null) {
            aggregateFilter = Optional.of(AggregateFilters.parseAggregateFilter(ctx.filter, context));
        }
        if (ctx.alias != null) {
            alias = Optional.of(ctx.alias.getText());
        }
        return new GroupByEntry(groupBy, aggregateFilter, alias);
    }

    public static GroupBy parseGroupBy(
            final JQLParser.GroupByElementContext groupByElementContext,
            final Query.Context context) {
        final GroupBy[] ref = new GroupBy[1];

        groupByElementContext.enterRule(new JQLBaseListener() {
            private void accept(GroupBy value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDayOfWeekGroupBy(JQLParser.DayOfWeekGroupByContext ctx) {
                if (ctx.hasParens == null) {
                    context.warn.accept("DAYOFWEEK is deprecated -- should be DAYOFWEEK().");
                }
                accept(new GroupBy.GroupByDayOfWeek());
            }

            @Override
            public void enterQuantilesGroupBy(JQLParser.QuantilesGroupByContext ctx) {
                accept(new GroupBy.GroupByQuantiles(parseIdentifier(ctx.field), Integer.parseInt(ctx.NAT().getText())));
            }

            @Override
            public void enterTopTermsGroupBy(JQLParser.TopTermsGroupByContext ctx) {
                final JQLParser.TopTermsGroupByElemContext ctx2 = ctx.topTermsGroupByElem();
                final Positioned<String> field = parseIdentifier(ctx2.field);
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.absent();
                }
                Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    metric = Optional.of(AggregateMetrics.parseAggregateMetric(ctx2.metric, context));
                } else {
                    metric = Optional.absent();
                }
                if (metric.isPresent() && ctx2.order != null) {
                    if (VALID_ORDERINGS.contains(ctx2.order.getText().toLowerCase())) {
                        metric = Optional.<AggregateMetric>of(new AggregateMetric.Negate(metric.get()));
                    }
                }
                accept(new GroupBy.GroupByField(field, Optional.<AggregateFilter>absent(), limit, metric, false, false));
            }

            @Override
            public void enterGroupByFieldIn(JQLParser.GroupByFieldInContext ctx) {
                if (ctx.not != null) {
                    final AggregateFilter filter = AggregateFilters.aggregateInHelper(ctx.terms, true);
                    accept(new GroupBy.GroupByField(parseIdentifier(ctx.field), Optional.of(filter), Optional.absent(), Optional.absent(), ctx.withDefault != null, false));
                } else {
                    final List<Term> terms = new ArrayList<>();
                    boolean anyString = false;
                    for (final JQLParser.TermValContext term : ctx.terms) {
                        final Term t = Term.parseTerm(term);
                        anyString |= !t.isIntTerm;
                        terms.add(t);
                    }
                    final List<String> strings;
                    final LongList ints;
                    if (anyString) {
                        strings = new ArrayList<>();
                        for (final Term term : terms) {
                            strings.add(term.isIntTerm ? String.valueOf(term.intTerm) : term.stringTerm);
                        }
                        ints = LongLists.EMPTY_LIST;
                    } else {
                        ints = new LongArrayList();
                        for (final Term term : terms) {
                            ints.add(term.intTerm);
                        }
                        strings = Collections.emptyList();
                    }
                    final boolean withDefault = ctx.withDefault != null;
                    accept(new GroupBy.GroupByFieldIn(parseIdentifier(ctx.field), ints, strings, withDefault));
                }
            }

            @Override
            public void enterMetricGroupBy(JQLParser.MetricGroupByContext ctx) {
                final DocMetric metric;
                final long min;
                final long max;
                final long interval;
                final boolean excludeGutters;
                final boolean withDefault;
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
                } else if (ctx.groupByMetricEnglish() != null) {
                    final JQLParser.GroupByMetricEnglishContext ctx2 = ctx.groupByMetricEnglish();
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), context);
                    min = Long.parseLong(ctx2.min.getText());
                    max = Long.parseLong(ctx2.max.getText());
                    interval = Long.parseLong(ctx2.interval.getText());
                    excludeGutters = false;
                    withDefault = ctx2.withDefault != null;
                } else {
                    throw new UnsupportedOperationException("Oh no! Someone changed the parser but not the consumer!");
                }
                if (excludeGutters && withDefault) {
                    throw new IllegalArgumentException("Can't use both excludeGutters and withDefault explicitly! Just use with default.");
                }
                accept(new GroupBy.GroupByMetric(metric, min, max, interval, excludeGutters || withDefault, withDefault));
            }

            @Override
            public void enterTimeGroupBy(JQLParser.TimeGroupByContext ctx) {
                final Optional<Positioned<String>> timeField;
                if (ctx.groupByTime().timeField != null) {
                    timeField = Optional.of(parseIdentifier(ctx.groupByTime().timeField));
                } else {
                    timeField = Optional.absent();
                }

                final boolean isRelative = ctx.groupByTime().isRelative != null;

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
                    timeFormat = Optional.absent();
                }

                final List<Pair<Integer, TimeUnit>> pairs = TimePeriods.parseTimePeriod(ctx.groupByTime().timePeriod(), ctx.useLegacy);
                long millisSum = 0L;
                for (final Pair<Integer, TimeUnit> pair : pairs) {
                    final int coeff = pair.getFirst();
                    final TimeUnit unit = pair.getSecond();

                    if (unit == TimeUnit.BUCKETS && pairs.size() > 1) {
                        throw new IqlKnownException.ParseErrorException("Can't group by buckets and also other time units in the same time group by");
                    } else if (unit == TimeUnit.MONTH && pairs.size() > 1) {
                        throw new IqlKnownException.ParseErrorException("Can't group by months and also other time units in the same time group by");
                    } else if (unit == TimeUnit.MONTH && coeff != 1) {
                        throw new IqlKnownException.ParseErrorException("Month group by must be 1 month for time group-by.");
                    }

                    if (unit == TimeUnit.BUCKETS) {
                        accept(new GroupBy.GroupByTimeBuckets(coeff, timeField, timeFormat));
                        return;
                    }
                    if (unit == TimeUnit.MONTH) {
                        accept(new GroupBy.GroupByMonth(timeField, timeFormat));
                        return;
                    }

                    millisSum += coeff * unit.millis;
                }

                accept(new GroupBy.GroupByTime(millisSum, timeField, timeFormat, isRelative));
            }

            @Override
            public void enterFieldGroupBy(JQLParser.FieldGroupByContext ctx) {
                final JQLParser.GroupByFieldContext ctx2 = ctx.groupByField();
                final Positioned<String> field = parseIdentifier(ctx2.field);
                final boolean reverseOrder = ctx2.order != null && ctx2.order.getText().equalsIgnoreCase("bottom");
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.absent();
                }
                final Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    AggregateMetric theMetric = AggregateMetrics.parseAggregateMetric(ctx2.metric, context);
                    if (reverseOrder) {
                        if (theMetric instanceof AggregateMetric.DocStats) {
                            theMetric = new AggregateMetric.DocStats(
                                    new DocMetric.Negate(((AggregateMetric.DocStats) theMetric).docMetric));
                        } else {
                            theMetric = new AggregateMetric.Negate(theMetric);
                        }
                    }
                    metric = Optional.of(theMetric);
                } else {
                    if (reverseOrder) {
                        metric = Optional.<AggregateMetric>of(new AggregateMetric.DocStats(new DocMetric.Negate(new DocMetric.Count())));
                    } else {
                        metric = Optional.absent();
                    }
                }
                final Optional<AggregateFilter> filter;
                if (ctx2.filter != null) {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx2.filter, context));
                } else {
                    filter = Optional.absent();
                }
                final boolean withDefault = ctx2.withDefault != null;
                final boolean forceNonStreaming = ctx2.forceNonStreaming != null;
                accept(new GroupBy.GroupByField(field, filter, limit, metric, withDefault, forceNonStreaming));
            }

            @Override
            public void enterDatasetGroupBy(JQLParser.DatasetGroupByContext ctx) {
                accept(new GroupBy.GroupBySessionName());
            }

            @Override
            public void enterPredicateGroupBy(JQLParser.PredicateGroupByContext ctx) {
                final DocFilter filter = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), context);
                accept(new GroupBy.GroupByPredicate(filter));
            }

            @Override
            public void enterRandomGroupBy(JQLParser.RandomGroupByContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
                final int k = Integer.parseInt(ctx.k.getText());
                final String salt;
                if (ctx.salt == null) {
                    salt = "DEFAULT SALT";
                } else {
                    salt = ParserCommon.unquote(ctx.salt.getText());
                }
                accept(new GroupBy.GroupByRandom(field, k, salt));
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
                accept(new GroupBy.GroupByRandomMetric(metric, k, salt));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Failed to handle group by: " + groupByElementContext.getText());
        }

        ref[0].copyPosition(groupByElementContext);

        return ref[0];
    }
}
