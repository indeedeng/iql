package com.indeed.squall.iql2.language.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateFilters;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.AggregateMetrics;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.DocMetrics;
import com.indeed.squall.iql2.language.JQLBaseListener;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.TimeUnit;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupBys {

    public static final ImmutableSet<String> VALID_ORDERINGS = ImmutableSet.of("bottom", "descending", "desc");

    public static List<GroupBy> parseGroupBys(JQLParser.GroupByContentsContext groupByContentsContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
        final List<JQLParser.GroupByElementContext> elements = groupByContentsContext.groupByElement();
        final List<GroupBy> result = new ArrayList<>(elements.size());
        for (final JQLParser.GroupByElementContext element : elements) {
            result.add(parseGroupBy(element, datasetToKeywordAnalyzerFields, datasetToIntFields));
        }
        return result;
    }

    public static GroupBy parseGroupBy(JQLParser.GroupByElementContext groupByElementContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
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
                accept(new GroupBy.GroupByDayOfWeek());
            }

            @Override
            public void enterQuantilesGroupBy(JQLParser.QuantilesGroupByContext ctx) {
                accept(new GroupBy.GroupByQuantiles(ctx.field.getText(), Integer.parseInt(ctx.INT().getText())));
            }

            @Override
            public void enterTopTermsGroupBy(JQLParser.TopTermsGroupByContext ctx) {
                final JQLParser.TopTermsGroupByElemContext ctx2 = ctx.topTermsGroupByElem();
                final String field = ctx2.field.getText();
                final Optional<Long> limit;
                if (ctx2.limit != null) {
                    limit = Optional.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = Optional.absent();
                }
                Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    metric = Optional.of(AggregateMetrics.parseAggregateMetric(ctx2.metric, datasetToKeywordAnalyzerFields, datasetToIntFields));
                } else {
                    metric = Optional.absent();
                }
                if (metric.isPresent() && ctx2.order != null) {
                    if (VALID_ORDERINGS.contains(ctx2.order.getText())) {
                        metric = Optional.<AggregateMetric>of(new AggregateMetric.Negate(metric.get()));
                    }
                }
                accept(new GroupBy.GroupByField(field, Optional.<AggregateFilter>absent(), limit, metric, false, false));
            }

            @Override
            public void enterGroupByFieldIn(JQLParser.GroupByFieldInContext ctx) {
                final AggregateFilter filter = AggregateFilters.aggregateInHelper(ctx.terms, ctx.not != null);
                accept(new GroupBy.GroupByField(ctx.field.getText(), Optional.of(filter), Optional.<Long>absent(), Optional.<AggregateMetric>absent(), ctx.withDefault != null, false));
            }

            @Override
            public void enterMetricGroupBy(JQLParser.MetricGroupByContext ctx) {
                final DocMetric metric;
                final long min;
                final long max;
                final long interval;
                final boolean excludeGutters;
                if (ctx.groupByMetric() != null) {
                    final JQLParser.GroupByMetricContext ctx2 = ctx.groupByMetric();
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
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
                } else if (ctx.groupByMetricEnglish() != null) {
                    final JQLParser.GroupByMetricEnglishContext ctx2 = ctx.groupByMetricEnglish();
                    metric = DocMetrics.parseDocMetric(ctx2.docMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
                    min = Long.parseLong(ctx2.min.getText());
                    max = Long.parseLong(ctx2.max.getText());
                    interval = Long.parseLong(ctx2.interval.getText());
                    excludeGutters = false;
                } else {
                    throw new UnsupportedOperationException("Oh no! Someone changed the parser but not the consumer!");
                }
                accept(new GroupBy.GroupByMetric(metric, min, max, interval, excludeGutters));
            }

            @Override
            public void enterTimeGroupBy(JQLParser.TimeGroupByContext ctx) {
                final Optional<String> timeField;
                if (ctx.groupByTime().timeField != null) {
                    timeField = Optional.of(ctx.groupByTime().timeField.getText());
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

                final List<Pair<Integer, TimeUnit>> pairs = ParserCommon.parseTimePeriod(ctx.groupByTime().timePeriod());
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
                final String field = ctx2.field.getText();
                final boolean reverseOrder;
                if (ctx2.order != null && ctx2.order.getText().equals("bottom")) {
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
                    AggregateMetric theMetric = AggregateMetrics.parseAggregateMetric(ctx2.metric, datasetToKeywordAnalyzerFields, datasetToIntFields);
                    if (reverseOrder) {
                        theMetric = new AggregateMetric.Negate(theMetric);
                    }
                    metric = Optional.of(theMetric);
                } else {
                    metric = Optional.absent();
                }
                final Optional<AggregateFilter> filter;
                if (ctx2.filter != null) {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx2.filter, datasetToKeywordAnalyzerFields, datasetToIntFields));
                } else {
                    filter = Optional.absent();
                }
                final boolean withDefault = ctx2.withDefault != null;
                final boolean forceNonStreaming = ctx2.forceNonStreaming != null;
                accept(new GroupBy.GroupByField(field, filter, limit, metric, withDefault, forceNonStreaming));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Failed to handle group by: " + groupByElementContext.getText());
        }
        return ref[0];
    }
}
