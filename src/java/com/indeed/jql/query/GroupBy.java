package com.indeed.jql.query;

import com.indeed.jql.AggregateFilter;
import com.indeed.jql.AggregateMetric;
import com.indeed.jql.DocMetric;
import com.indeed.jql.JQLBaseListener;
import com.indeed.jql.JQLParser;
import com.indeed.jql.Main;
import com.indeed.jql.TimeUnit;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public interface GroupBy<F,M> {
    static List<GroupBy<AggregateFilter, AggregateMetric>> parseGroupBys(JQLParser.GroupByContentsContext groupByContentsContext) {
        final List<JQLParser.GroupByElementContext> elements = groupByContentsContext.groupByElement();
        final List<GroupBy<AggregateFilter, AggregateMetric>> result = new ArrayList<>(elements.size());
        for (final JQLParser.GroupByElementContext element : elements) {
            result.add(parseGroupBy(element));
        }
        return result;
    }

    static GroupBy<AggregateFilter, AggregateMetric> parseGroupBy(JQLParser.GroupByElementContext groupByElementContext) {
        final GroupBy<AggregateFilter, AggregateMetric>[] ref = new GroupBy[1];

        groupByElementContext.enterRule(new JQLBaseListener() {
            private void accept(GroupBy<AggregateFilter, AggregateMetric> value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDayOfWeekGroupBy(@NotNull JQLParser.DayOfWeekGroupByContext ctx) {
                accept(new GroupByDayOfWeek<>());
            }

            @Override
            public void enterQuantilesGroupBy(@NotNull JQLParser.QuantilesGroupByContext ctx) {
                accept(new GroupByQuantiles<>(ctx.field.getText(), Integer.parseInt(ctx.INT().getText())));
            }

            @Override
            public void enterTopTermsGroupBy(@NotNull JQLParser.TopTermsGroupByContext ctx) {
                final JQLParser.TopTermsGroupByElemContext ctx2 = ctx.topTermsGroupByElem();
                final String field = ctx2.field.getText();
                final OptionalLong limit;
                if (ctx2.limit != null) {
                    limit = OptionalLong.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = OptionalLong.empty();
                }
                Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    metric = Optional.of(AggregateMetric.parseAggregateMetric(ctx2.metric));
                } else {
                    metric = Optional.empty();
                }
                if (metric.isPresent() && ctx2.order != null) {
                    // TODO: Hoist this out, and use an ImmutableSet.
                    if (new HashSet<>(Arrays.asList("bottom", "descending", "desc")).contains(ctx2.order.getText())) {
                        metric = Optional.of(new AggregateMetric.Negate(metric.get()));
                    }
                }
                accept(new GroupByField<>(field, Optional.empty(), limit, metric, false));
            }

            @Override
            public void enterGroupByFieldIn(@NotNull JQLParser.GroupByFieldInContext ctx) {
                final AggregateFilter filter = AggregateFilter.aggregateInHelper(ctx.terms, ctx.not != null);
                accept(new GroupByField<>(ctx.field.getText(), Optional.of(filter), OptionalLong.empty(), Optional.<AggregateMetric>empty(), ctx.withDefault != null));
            }

            @Override
            public void enterMetricGroupBy(@NotNull JQLParser.MetricGroupByContext ctx) {
                final DocMetric metric;
                final long min;
                final long max;
                final long interval;
                final boolean excludeGutters;
                if (ctx.groupByMetric() != null) {
                    final JQLParser.GroupByMetricContext ctx2 = ctx.groupByMetric();
                    metric = DocMetric.parseDocMetric(ctx2.docMetric());
                    min = Long.parseLong(ctx2.min.getText());
                    max = Long.parseLong(ctx2.max.getText());
                    interval = Long.parseLong(ctx2.interval.getText());
                    excludeGutters = false;
                } else if (ctx.groupByMetricEnglish() != null) {
                    final JQLParser.GroupByMetricEnglishContext ctx2 = ctx.groupByMetricEnglish();
                    metric = DocMetric.parseDocMetric(ctx2.docMetric());
                    min = Long.parseLong(ctx2.min.getText());
                    max = Long.parseLong(ctx2.max.getText());
                    interval = Long.parseLong(ctx2.interval.getText());
                    excludeGutters = false;
                } else {
                    throw new UnsupportedOperationException("Oh no! Someone changed the parser but not the consumer!");
                }
                accept(new GroupByMetric<>(metric, min, max, interval, excludeGutters));
            }

            @Override
            public void enterTimeGroupBy(@NotNull JQLParser.TimeGroupByContext ctx) {
                final Optional<String> timeField;
                if (ctx.groupByTime().timeField != null) {
                    timeField = Optional.of(ctx.groupByTime().timeField.getText());
                } else {
                    timeField = Optional.empty();
                }

                final List<Pair<Integer, TimeUnit>> pairs = Main.parseTimePeriod(ctx.groupByTime().timePeriod());
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
                        accept(new GroupByTimeBuckets<>(coeff, timeField, Optional.empty()));
                        return;
                    }
                    if (unit == TimeUnit.MONTH) {
                        accept(new GroupByMonth<>(timeField, Optional.<String>empty()));
                        return;
                    }

                    millisSum += coeff * unit.millis;
                }

                accept(new GroupByTime<>(millisSum, timeField, Optional.<String>empty()));
            }

            @Override
            public void enterFieldGroupBy(@NotNull JQLParser.FieldGroupByContext ctx) {
                final JQLParser.GroupByFieldContext ctx2 = ctx.groupByField();
                final String field = ctx2.field.getText();
                final boolean reverseOrder;
                if (ctx2.order != null && ctx2.order.getText().equals("bottom")) {
                    reverseOrder = true;
                } else {
                    reverseOrder = false;
                }
                final OptionalLong limit;
                if (ctx2.limit != null) {
                    limit = OptionalLong.of(Long.parseLong(ctx2.limit.getText()));
                } else {
                    limit = OptionalLong.empty();
                }
                final Optional<AggregateMetric> metric;
                if (ctx2.metric != null) {
                    AggregateMetric theMetric = AggregateMetric.parseAggregateMetric(ctx2.metric);
                    if (reverseOrder) {
                        theMetric = new AggregateMetric.Negate(theMetric);
                    }
                    metric = Optional.of(theMetric);
                } else {
                    metric = Optional.empty();
                }
                final Optional<AggregateFilter> filter;
                if (ctx2.filter != null) {
                    filter = Optional.of(AggregateFilter.parseAggregateFilter(ctx2.filter));
                } else {
                    filter = Optional.empty();
                }
                final boolean withDefault = ctx2.withDefault != null;
                accept(new GroupByField<>(field, filter, limit, metric, withDefault));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Failed to handle group by: " + groupByElementContext.getText());
        }
        return ref[0];
    }

    class GroupByMetric<F,M> implements GroupBy<F,M> {
        private final DocMetric metric;
        private final long min;
        private final long max;
        private final long interval;
        private final boolean excludeGutters;

        public GroupByMetric(DocMetric metric, long min, long max, long interval, boolean excludeGutters) {
            this.metric = metric;
            this.min = min;
            this.max = max;
            this.interval = interval;
            this.excludeGutters = excludeGutters;
        }
    }

    class GroupByTime<F,M> implements GroupBy<F,M> {
        private final long periodMillis;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTime(long periodMillis, Optional<String> field, Optional<String> format) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
        }
    }

    class GroupByTimeBuckets<F,M> implements GroupBy<F,M> {
        private final int numBuckets;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<String> field, Optional<String> format) {
            this.numBuckets = numBuckets;
            this.field = field;
            this.format = format;
        }
    }

    class GroupByMonth<F,M> implements GroupBy<F,M> {
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
        }
    }

    class GroupByField<F,M> implements GroupBy<F,M> {
        private final String field;
        private final Optional<F> filter;
        private final OptionalLong limit;
        private final Optional<M> metric;
        private final boolean withDefault;

        public GroupByField(String field, Optional<F> filter, OptionalLong limit, Optional<M> metric, boolean withDefault) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.metric = metric;
            this.withDefault = withDefault;
        }
    }

    class GroupByDayOfWeek<F,M> implements GroupBy<F,M> {}
    class GroupBySessionName<F,M> implements GroupBy<F,M> {}

    class GroupByQuantiles<F,M> implements GroupBy<F,M> {
        private final String field;
        private final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }
    }
}
