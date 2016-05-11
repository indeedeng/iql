package com.indeed.squall.iql2.language.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateFilters;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.AggregateMetrics;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.DocMetrics;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.Identifiers;
import com.indeed.squall.iql2.language.JQLBaseListener;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.Term;
import com.indeed.squall.iql2.language.TimePeriods;
import com.indeed.squall.iql2.language.TimeUnit;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.util.core.Pair;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

public class GroupBys {

    public static final ImmutableSet<String> VALID_ORDERINGS = ImmutableSet.of("bottom", "descending", "desc");

    public static List<GroupByMaybeHaving> parseGroupBys(JQLParser.GroupByContentsContext groupByContentsContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        final List<JQLParser.GroupByElementWithHavingContext> elements = groupByContentsContext.groupByElementWithHaving();
        final List<GroupByMaybeHaving> result = new ArrayList<>(elements.size());
        for (final JQLParser.GroupByElementWithHavingContext element : elements) {
            result.add(parseGroupByMaybeHaving(element, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
        }
        return result;
    }

    public static GroupByMaybeHaving parseGroupByMaybeHaving(
            final JQLParser.GroupByElementWithHavingContext ctx,
            final Map<String, Set<String>> datasetToKeywordAnalyzerFields,
            final Map<String, Set<String>> datasetToIntFields,
            final Consumer<String> warn, WallClock clock
    ) {
        final GroupBy groupBy = parseGroupBy(ctx.groupByElement(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
        if (ctx.filter != null) {
            return GroupByMaybeHaving.of(groupBy, AggregateFilters.parseAggregateFilter(ctx.filter, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
        } else {
            return GroupByMaybeHaving.of(groupBy);
        }
    }

    public static GroupBy parseGroupBy(JQLParser.GroupByElementContext groupByElementContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields, final Consumer<String> warn, final WallClock clock) {
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
                    warn.accept("DAYOFWEEK is deprecated -- should be DAYOFWEEK().");
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
                final String field = parseIdentifier(ctx2.field);
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.absent();
                }
                Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    metric = Optional.of(AggregateMetrics.parseAggregateMetric(ctx2.metric, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
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
                AggregateFilter filter = null;
                for (final JQLParser.TermValContext term : ctx.terms) {
                    final AggregateFilter.TermIs f = new AggregateFilter.TermIs(Term.parseTerm(term));
                    if (filter == null) {
                        filter = f;
                    } else {
                        filter = new AggregateFilter.Or(filter, f);
                    }
                }
                if (filter == null) {
                    filter = new AggregateFilter.Never();
                }
                if (ctx.not != null) {
                    filter = new AggregateFilter.Not(filter);
                }

                final boolean withDefault = ctx.withDefault != null;

                accept(new GroupBy.GroupByField(parseIdentifier(ctx.field), Optional.of(filter), Optional.<Long>absent(), Optional.<AggregateMetric>absent(), withDefault, false));
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
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
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
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
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
                final Optional<String> timeField;
                if (ctx.groupByTime().timeField != null) {
                    timeField = Optional.of(parseIdentifier(ctx.groupByTime().timeField));
                } else {
                    timeField = Optional.absent();
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
                    timeFormat = Optional.absent();
                }

                final List<Pair<Integer, TimeUnit>> pairs = TimePeriods.parseTimePeriod(ctx.groupByTime().timePeriod());
                long millisSum = 0L;
                for (final Pair<Integer, TimeUnit> pair : pairs) {
                    final int coeff = pair.getFirst();
                    final TimeUnit unit = pair.getSecond();

                    if (unit == TimeUnit.BUCKETS && pairs.size() > 1) {
                        throw new IllegalArgumentException("Can't group by buckets and also other time units in the same time group by");
                    } else if (unit == TimeUnit.MONTH && pairs.size() > 1) {
                        throw new IllegalArgumentException("Can't group by months and also other time units in the same time group by");
                    } else if (unit == TimeUnit.MONTH && coeff != 1) {
                        throw new IllegalArgumentException("Month group by must be 1 month for time group-by.");
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

                accept(new GroupBy.GroupByTime(millisSum, timeField, timeFormat));
            }

            @Override
            public void enterFieldGroupBy(JQLParser.FieldGroupByContext ctx) {
                final JQLParser.GroupByFieldContext ctx2 = ctx.groupByField();
                final String field = parseIdentifier(ctx2.field);
                final boolean reverseOrder;
                if (ctx2.order != null && ctx2.order.getText().equalsIgnoreCase("bottom")) {
                    reverseOrder = true;
                } else {
                    reverseOrder = false;
                }
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.absent();
                }
                final Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    AggregateMetric theMetric = AggregateMetrics.parseAggregateMetric(ctx2.metric, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                    if (reverseOrder) {
                        theMetric = new AggregateMetric.Negate(theMetric);
                    }
                    metric = Optional.of(theMetric);
                } else {
                    if (reverseOrder) {
                        metric = Optional.<AggregateMetric>of(new AggregateMetric.Negate(new AggregateMetric.DocStats(new DocMetric.Count())));
                    } else {
                        metric = Optional.absent();
                    }
                }
                final Optional<AggregateFilter> filter;
                if (ctx2.filter != null) {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx2.filter, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                } else {
                    filter = Optional.absent();
                }
                final boolean withDefault = ctx2.withDefault != null;
                final boolean forceNonStreaming = ctx2.forceNonStreaming != null;
                accept(new GroupBy.GroupByField(field, filter, limit, metric, withDefault, forceNonStreaming));
            }

            @Override
            public void enterPredicateGroupBy(JQLParser.PredicateGroupByContext ctx) {
                final DocFilter filter = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, null, warn, clock);
                accept(new GroupBy.GroupByPredicate(filter));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Failed to handle group by: " + groupByElementContext.getText());
        }
        return ref[0];
    }
}
