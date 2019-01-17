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

package com.indeed.iql.web.print;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.Positional;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldResolver;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.util.ParserUtil;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class PrettyPrint {
    private static final Function<String, String> RENDER_STRING = new Function<String, String>() {
        public String apply(String s) {
            return "\"" + stringEscape(s) + "\"";
        }
    };

    public static void main(String[] args) {
        final DatasetsMetadata datasetsMetadata = DatasetsMetadata.empty();
        final String pretty = prettyPrint("from jobsearch /* index name! */ 2d 1d as blah, mobsearch where foo:\"3\" /* hi */ country:us (oji + ojc) = 10 group by something select somethingElse /* after */, /* before */ distinct(thing) /*eof*/", true, datasetsMetadata);
        System.out.println("pretty = " + pretty);
    }

    @Nonnull
    public static String prettyPrint(String q, boolean useLegacy, DatasetsMetadata datasetsMetadata) {
        JQLParser.QueryContext queryContext = Queries.parseQueryContext(q, useLegacy);
        Consumer<String> consumer = new Consumer<String>() {
            @Override
            public void accept(String s) {
            }
        };
        WallClock clock = new StoppedClock();
        Query query = Query.parseQuery(queryContext, datasetsMetadata, Collections.emptySet(), consumer, clock);
        return prettyPrint(queryContext, query, datasetsMetadata, consumer, clock);
    }

    private static String prettyPrint(JQLParser.QueryContext queryContext, Query query, DatasetsMetadata datasetsMetadata, Consumer<String> consumer, WallClock clock) {
        final FieldResolver fieldResolver = FieldResolver.build(queryContext, queryContext.fromContents(), datasetsMetadata);
        final PrettyPrint prettyPrint = new PrettyPrint(queryContext, datasetsMetadata, fieldResolver.universalScope());
        prettyPrint.pp(query, consumer, clock);
        while (prettyPrint.sb.charAt(prettyPrint.sb.length() - 1) == '\n') {
            prettyPrint.sb.setLength(prettyPrint.sb.length() - 1);
        }
        return prettyPrint.sb.toString();
    }

    private final CharStream inputStream;
    private final StringBuilder sb = new StringBuilder();
    private final DatasetsMetadata datasetsMetadata;
    private final ScopedFieldResolver fieldResolver;

    // This is used to prevent the situation where multiple layers of abstraction all share the same comment
    // causing it to be printed multiple times as we pp() each point in the tree.
    // A bit of a hack. Can probably be removed by making the wrappers not pp(), but that's effort.
    private final Set<Interval> seenCommentIntervals = new HashSet<>();

    private PrettyPrint(JQLParser.QueryContext queryContext, final DatasetsMetadata datasetsMetadata, final ScopedFieldResolver fieldResolver) {
        this.inputStream = queryContext.start.getInputStream();
        this.datasetsMetadata = datasetsMetadata;
        this.fieldResolver = fieldResolver;
    }

    private String getText(Positional positional) {
        final StringBuilder sb = new StringBuilder();
        appendCommentBeforeText(positional, sb);
        sb.append(inputStream.getText(new Interval(positional.getStart().startIndex, positional.getEnd().stopIndex)));
        appendCommentAfterText(positional, sb);
        return sb.toString();
    }

    private void appendCommentAfterText(Positional positional, StringBuilder sb) {
        final Optional<Interval> optionalInterval = ParserUtil.getNextNode(positional.getParserRuleContext());
        if (!optionalInterval.isPresent()) {
            return;
        }

        final Interval interval = optionalInterval.get();
        if (seenCommentIntervals.contains(interval)) {
            return;
        }
        seenCommentIntervals.add(interval);
        final String comment = inputStream.getText(interval).trim();
        if (comment.isEmpty()) {
            return;
        }
        sb.append(' ').append(comment);
    }

    private void appendCommentBeforeText(Positional positional, StringBuilder sb) {
        final Optional<Interval> optionalInterval = ParserUtil.getPreviousNode(positional.getParserRuleContext());
        if (!optionalInterval.isPresent()) {
            return;
        }

        final Interval interval = optionalInterval.get();
        if (seenCommentIntervals.contains(interval)) {
            return;
        }
        seenCommentIntervals.add(interval);
        final String comment = inputStream.getText(interval).trim();
        if (comment.isEmpty()) {
            return;
        }
        sb.append(comment).append(' ');
    }

    private void pp(Query query, Consumer<String> consumer, WallClock clock) {
        sb.append("FROM ");
        final boolean multiDataSets = query.datasets.size() > 1;

        DateTime firstDatasetStart = null;
        DateTime firstDatasetEnd = null;

        for (final Dataset dataset : query.datasets) {
            if (multiDataSets) {
                sb.append("\n    ");
            }

            appendCommentBeforeText(dataset, sb);

            sb.append(getText(dataset.dataset));
            if (!dataset.startInclusive.unwrap().equals(firstDatasetStart)
                    || !dataset.endExclusive.unwrap().equals(firstDatasetEnd)) {
                sb.append(' ').append(getText(dataset.startInclusive));
                sb.append(' ').append(getText(dataset.endExclusive));
            }
            if (firstDatasetStart == null && firstDatasetEnd == null) {
                firstDatasetStart = dataset.startInclusive.unwrap();
                firstDatasetEnd = dataset.endExclusive.unwrap();
            }
            if (dataset.alias.isPresent()) {
                sb.append(" as ").append(getText(dataset.alias.get()));
            }
            if (!dataset.fieldAliases.isEmpty()) {
                sb.append(" aliasing (");
                final ArrayList<Map.Entry<Positioned<String>, Positioned<String>>> sortedAliases = Lists.newArrayList(dataset.fieldAliases.entrySet());
                Collections.sort(sortedAliases, new Comparator<Map.Entry<Positioned<String>, Positioned<String>>>() {
                    @Override
                    public int compare(Map.Entry<Positioned<String>, Positioned<String>> o1, Map.Entry<Positioned<String>, Positioned<String>> o2) {
                        return o1.getKey().unwrap().compareTo(o2.getKey().unwrap());
                    }
                });
                for (final Map.Entry<Positioned<String>, Positioned<String>> entry : sortedAliases) {
                    sb.append(getText(entry.getKey())).append(" AS ").append(getText(entry.getValue()));
                }
                sb.append(")");
            }

            appendCommentAfterText(dataset, sb);
        }
        sb.append('\n');

        sb.append("WHERE ");
        if (query.filter.isPresent()) {
            final List<DocFilter> filters = DocFilter.And.unwrap(Collections.singletonList(query.filter.get()));
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sb.append(' ');
                }
                pp(filters.get(i), consumer, clock);
            }
        }
        sb.append('\n');

        sb.append("GROUP BY ");
        final boolean isMultiGroupBy = query.groupBys.size() > 1;
        boolean isFirstGroupBy = true;
        for (final GroupByEntry groupBy : query.groupBys) {
            if (isFirstGroupBy && isMultiGroupBy) {
                sb.append("\n    ");
            } else if (!isFirstGroupBy) {
                sb.append("\n  , ");
            }
            isFirstGroupBy = false;
            pp(groupBy, consumer, clock);
        }
        sb.append('\n');

        sb.append("SELECT ");
        final boolean isMultiSelect = query.selects.size() > 1;
        boolean isFirstSelect = true;
        for (final AggregateMetric select : query.selects) {
            if (isFirstSelect && isMultiSelect) {
                sb.append("\n    ");
            } else if (!isFirstSelect) {
                sb.append("\n  , ");
            }
            isFirstSelect = false;
            pp(select, consumer, clock);
        }
        sb.append('\n');
    }

    // TODO: prettyprint comments
    private void pp(GroupByEntry groupBy, Consumer<String> consumer, WallClock clock) {
        pp(groupBy.groupBy, consumer, clock);
        if (groupBy.filter.isPresent()) {
            sb.append(" HAVING ");
            pp(groupBy.filter.get(), consumer, clock);
        }
    }

    boolean isIQL2Consistent(AbstractPositional positional, Consumer<String> consumer, WallClock clock) {
        try {
            final String rawString = getText(positional);
            final AbstractPositional positionalIQL2;
            final Query.Context context = new Query.Context(null, datasetsMetadata, null, consumer, clock, fieldResolver);
            if (positional instanceof GroupBy) {
                positionalIQL2 = Queries.parseGroupBy(rawString, false, context);
            } else if (positional instanceof AggregateFilter) {
                positionalIQL2 = Queries.parseAggregateFilter(rawString, false, context);
            } else if (positional instanceof AggregateMetric) {
                positionalIQL2 = Queries.parseAggregateMetric(rawString, false, context);
            } else if (positional instanceof DocFilter) {
                positionalIQL2 = Queries.parseDocFilter(rawString, false, context);
            } else if (positional instanceof DocMetric) {
                positionalIQL2 = Queries.parseDocMetric(rawString, false, context);
            } else {
                throw new IllegalArgumentException("unrecognized type");
            }
            if (positionalIQL2.equals(positional)) {
                appendCommentBeforeText(positional, sb);
                //preserve as much as possible
                sb.append(rawString);
                appendCommentAfterText(positional, sb);
                return true;
            }
            return false;
        } catch (Exception e) {
            //can't parse with IQL2
            return false;
        }
    }

    private void pp(GroupBy groupBy, Consumer<String> consumer, WallClock clock) {
        if (isIQL2Consistent(groupBy, consumer, clock)) {
            return ;
        }

        appendCommentBeforeText(groupBy, sb);

        groupBy.visit(new GroupBy.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(GroupBy.GroupByMetric groupByMetric) {
                sb.append("bucket(");
                pp(groupByMetric.metric, consumer, clock);
                sb.append(", ").append(groupByMetric.min);
                sb.append(", ").append(groupByMetric.max);
                sb.append(", ").append(groupByMetric.interval);
                sb.append(", ").append(groupByMetric.excludeGutters ? "1" : "0");
                sb.append(')');
                if (groupByMetric.withDefault) {
                    sb.append(" with default");
                }
                return null;
            }

            private void timeFieldAndFormat(Optional<FieldSet> field, Optional<String> format) {
                if (field.isPresent() || format.isPresent()) {
                    if (format.isPresent()) {
                        sb.append(", ").append('"').append(stringEscape(format.get())).append('"');
                    } else {
                        sb.append(", default");
                    }
                    if (field.isPresent()) {
                        sb.append(", ").append(getText(field.get()));
                    }
                }
            }

            @Override
            public Void visit(GroupBy.GroupByTime groupByTime) {
                sb.append("time(");
                covertTimeMillisToDateString(groupByTime.periodMillis);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByTimeBuckets groupByTimeBuckets) {
                sb.append("time(");
                sb.append(groupByTimeBuckets.numBuckets).append('b');
                timeFieldAndFormat(groupByTimeBuckets.field, groupByTimeBuckets.format);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByMonth groupByMonth) {
                sb.append("time(1month");
                timeFieldAndFormat(groupByMonth.timeField, groupByMonth.timeFormat);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByFieldIn groupByFieldIn) {
                sb.append(getText(groupByFieldIn.field)).append(" IN (");
                if (!groupByFieldIn.intTerms.isEmpty()) {
                    Joiner.on(", ").appendTo(sb, groupByFieldIn.intTerms);
                }
                if (!groupByFieldIn.stringTerms.isEmpty()) {
                    Joiner.on(", ").appendTo(sb, Iterables.transform(groupByFieldIn.stringTerms, RENDER_STRING));
                }
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(final GroupBy.GroupByFieldInQuery groupByFieldInQuery) throws RuntimeException {
                sb.append(getText(groupByFieldInQuery.field));
                if (groupByFieldInQuery.isNegated) {
                    sb.append(" NOT");
                }
                sb.append(" IN (").append(groupByFieldInQuery.query).append(")");
                if (groupByFieldInQuery.withDefault) {
                    sb.append(" with default");
                }
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByField groupByField) {
                sb.append(getText(groupByField.field));
                if (groupByField.metric.isPresent() || groupByField.limit.isPresent()) {
                    sb.append('[');
                    if (groupByField.limit.isPresent()) {
                        sb.append(groupByField.limit.get());
                    }
                    if (groupByField.metric.isPresent()) {
                        sb.append(" BY ");
                        pp(groupByField.metric.get(), consumer, clock);
                    }
                    if (groupByField.filter.isPresent()) {
                        sb.append(" HAVING ");
                        pp(groupByField.filter.get(), consumer, clock);
                    }
                    sb.append(']');
                }
                if (groupByField.withDefault) {
                    sb.append(" with default");
                }
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByDayOfWeek groupByDayOfWeek) {
                sb.append("dayofweek");
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupBySessionName groupBySessionName) {
                throw new UnsupportedOperationException("There is currently no way to group by session name");
            }

            @Override
            public Void visit(GroupBy.GroupByQuantiles groupByQuantiles) {
                sb.append("quantiles(").append(getText(groupByQuantiles.field)).append(", ").append(groupByQuantiles.numBuckets).append(")");
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByPredicate groupByPredicate) {
                pp(groupByPredicate.docFilter, consumer, clock);
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByRandom groupByRandom) throws RuntimeException {
                sb.append("random(")
                        .append(getText(groupByRandom.field))
                        .append(", ")
                        .append(groupByRandom.k)
                        .append(", \"")
                        .append(groupByRandom.salt)
                        .append('"');
                return null;
            }

            @Override
            public Void visit(final GroupBy.GroupByRandomMetric groupByRandom) throws RuntimeException {
                sb.append("random(");
                pp(groupByRandom.metric, consumer, clock);
                sb.append(groupByRandom.k)
                        .append(", \"")
                        .append(groupByRandom.salt)
                        .append('"');
                return null;
            }
        });

        appendCommentAfterText(groupBy, sb);
    }

    private void covertTimeMillisToDateString(long periodMillis) {
        final TimeUnit[] timeUnits = new TimeUnit[]{TimeUnit.WEEK, TimeUnit.DAY, TimeUnit.HOUR, TimeUnit.MINUTE, TimeUnit.SECOND};
        for (TimeUnit timeUnit : timeUnits) {
            if (periodMillis >= timeUnit.millis) {
                final long val = periodMillis / timeUnit.millis;
                sb.append(String.format("%d%s", val, timeUnit.identifier));
                if (val > 1) {
                    sb.append("s");
                }
                periodMillis %= timeUnit.millis;
            }
        }
    }

    private void pp(AggregateFilter aggregateFilter, Consumer<String> consumer, WallClock clock) {
        if (isIQL2Consistent(aggregateFilter, consumer, clock)) {
            return ;
        }
        appendCommentBeforeText(aggregateFilter, sb);

        aggregateFilter.visit(new AggregateFilter.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(AggregateFilter.TermIs termIs) {
                sb.append("term()=");
                pp(termIs.term);
                return null;
            }

            @Override
            public Void visit(AggregateFilter.TermRegex termRegex) {
                sb.append("term()=~");
                pp(termRegex.term);
                return null;
            }

            private Void binop(AggregateMetric m1, String op, AggregateMetric m2) {
                sb.append('(');
                pp(m1, consumer, clock);
                sb.append(')');
                sb.append(op);
                sb.append('(');
                pp(m2, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateFilter.MetricIs metricIs) {
                return binop(metricIs.m1, "=", metricIs.m2);
            }

            @Override
            public Void visit(AggregateFilter.MetricIsnt metricIsnt) {
                return binop(metricIsnt.m1, "!=", metricIsnt.m2);
            }

            @Override
            public Void visit(AggregateFilter.Gt gt) {
                return binop(gt.m1, ">", gt.m2);
            }

            @Override
            public Void visit(AggregateFilter.Gte gte) {
                return binop(gte.m1, ">=", gte.m2);
            }

            @Override
            public Void visit(AggregateFilter.Lt lt) {
                return binop(lt.m1, "<", lt.m2);
            }

            @Override
            public Void visit(AggregateFilter.Lte lte) {
                return binop(lte.m1, "<=", lte.m2);
            }

            @Override
            public Void visit(final AggregateFilter.And and) {
                return visit(and, "and");
            }

            @Override
            public Void visit(final AggregateFilter.Or or) {
                return visit(or, "or");
            }

            @Override
            public Void visit(AggregateFilter.Not not) {
                sb.append("not(");
                pp(not.filter, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Regex regex) {
                sb.append(getText(regex.field));
                sb.append("=~");
                sb.append('"');
                sb.append(regexEscape(regex.regex));
                sb.append('"');
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Always always) {
                sb.append("true");
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Never never) {
                sb.append("false");
                return null;
            }

            @Override
            public Void visit(AggregateFilter.IsDefaultGroup isDefaultGroup) {
                throw new UnsupportedOperationException("What even is this operation?: " + isDefaultGroup);
            }

            private Void visit(final AggregateFilter.Multiple multiple, final String op) {
                for (int i = 0; i < multiple.filters.size(); i++) {
                    if (i > 0) {
                        sb.append(' ').append(op).append(' ');
                    }
                    sb.append('(');
                    pp(multiple.filters.get(i), consumer, clock);
                    sb.append(')');
                }
                return null;
            }
        });

        appendCommentAfterText(aggregateFilter, sb);
    }

    private void pp(AggregateMetric aggregateMetric, Consumer<String> consumer, WallClock clock) {
        if (aggregateMetric instanceof AggregateMetric.DocStats) {
            if (aggregateMetric.getStart() == null && ((AggregateMetric.DocStats) aggregateMetric).docMetric instanceof DocMetric.Count) {
                appendCommentBeforeText(aggregateMetric, sb);
                sb.append("count()");
                appendCommentAfterText(aggregateMetric, sb);
                return ;
            }
        }
        if (isIQL2Consistent(aggregateMetric, consumer, clock)) {
            return ;
        }
        appendCommentBeforeText(aggregateMetric, sb);

        aggregateMetric.visit(new AggregateMetric.Visitor<Void, RuntimeException>() {
            private Void binop(AggregateMetric.Binop binop, String op) {
                sb.append('(');
                pp(binop.m1, consumer, clock);
                sb.append(')');
                sb.append(op);
                sb.append('(');
                pp(binop.m2, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(final AggregateMetric.Add add) {
                for (int i = 0; i < add.metrics.size(); i++) {
                    if (i > 0) {
                        sb.append("+");
                    }
                    sb.append('(');
                    pp(add.metrics.get(i), consumer, clock);
                    sb.append(')');
                }
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Log log) {
                sb.append("log(");
                pp(log.m1, consumer, clock);
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Negate negate) {
                sb.append('-');
                sb.append('(');
                pp(negate.m1, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Abs abs) {
                sb.append("abs(");
                pp(abs.m1, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Subtract subtract) {
                return binop(subtract, "-");
            }

            @Override
            public Void visit(AggregateMetric.Multiply multiply) {
                return binop(multiply, "*");
            }

            @Override
            public Void visit(AggregateMetric.Divide divide) {
                return binop(divide, "/");
            }

            @Override
            public Void visit(AggregateMetric.Modulus modulus) {
                return binop(modulus, "%");
            }

            @Override
            public Void visit(AggregateMetric.Power power) {
                return binop(power, "^");
            }

            @Override
            public Void visit(AggregateMetric.Parent parent) {
                sb.append("parent(");
                pp(parent.metric, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Lag lag) {
                sb.append("lag(");
                sb.append(lag.lag);
                sb.append(", ");
                pp(lag.metric, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.DivideByCount divideByCount) {
                throw new UnsupportedOperationException("Shouldn't be rendering this.");
            }

            @Override
            public Void visit(AggregateMetric.IterateLag iterateLag) {
                return visit(new AggregateMetric.Lag(iterateLag.lag, iterateLag.metric));
            }

            @Override
            public Void visit(AggregateMetric.Window window) {
                sb.append("window(");
                sb.append(window.window);
                sb.append(", ");
                pp(window.metric, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Qualified qualified) {
                throw new UnsupportedOperationException("Uhh qualified uhhh ummm");
            }

            @Override
            public Void visit(AggregateMetric.DocStatsPushes docStatsPushes) {
                throw new UnsupportedOperationException("Shouldn't be rendering this.");
            }

            @Override
            public Void visit(AggregateMetric.DocStats docStats) {
                sb.append('[');
                pp(docStats.docMetric, consumer, clock);
                sb.append(']');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Constant constant) {
                sb.append(constant.value);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Percentile percentile) {
                sb.append("percentile(");
                sb.append(getText(percentile.field));
                sb.append(", ");
                sb.append(percentile.percentile);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Running running) {
                sb.append("running(");
                pp(running.metric, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Distinct distinct) {
                sb.append("distinct(");
                sb.append(getText(distinct.field));
                if (distinct.filter.isPresent()) {
                    sb.append(" HAVING ");
                    pp(distinct.filter.get(), consumer, clock);
                }
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Named named) {
                sb.append('(');
                pp(named.metric, consumer, clock);
                sb.append(')');
                sb.append(" as ");
                sb.append(getText(named.name));
                return null;
            }

            @Override
            public Void visit(final AggregateMetric.NeedsSubstitution needsSubstitution) throws RuntimeException {
                sb.append(needsSubstitution.substitutionName);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.GroupStatsLookup groupStatsLookup) {
                throw new UnsupportedOperationException("Shouldn't be rendering this");
            }

            @Override
            public Void visit(AggregateMetric.SumAcross sumAcross) {
                sb.append("sum_over(");
                pp(sumAcross.groupBy, consumer, clock);
                sb.append(", ");
                pp(sumAcross.metric, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.IfThenElse ifThenElse) {
                sb.append("if ");
                pp(ifThenElse.condition, consumer, clock);
                sb.append(" then ");
                pp(ifThenElse.trueCase, consumer, clock);
                sb.append(" else ");
                pp(ifThenElse.falseCase, consumer, clock);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.FieldMin fieldMin) {
                sb.append("field_min(");
                sb.append(getText(fieldMin.field));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.FieldMax fieldMax) {
                sb.append("field_max(");
                sb.append(getText(fieldMax.field));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Min min) {
                sb.append("min(");
                Joiner.on(", ").appendTo(sb, Iterables.transform(min.metrics, new Function<AggregateMetric, String>() {
                    public String apply(AggregateMetric aggregateMetric) {
                        final StringBuilder sb = new StringBuilder();
                        pp(aggregateMetric, consumer, clock);
                        return sb.toString();
                    }
                }));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Max max) {
                sb.append("max(");
                Joiner.on(", ").appendTo(sb, Iterables.transform(max.metrics, new Function<AggregateMetric, String>() {
                    public String apply(AggregateMetric aggregateMetric) {
                        final StringBuilder sb = new StringBuilder();
                        pp(aggregateMetric, consumer, clock);
                        return sb.toString();
                    }
                }));
                sb.append(')');
                return null;
            }
        });

        appendCommentAfterText(aggregateMetric, sb);
    }

    private void pp(DocFilter docFilter, Consumer<String> consumer, WallClock clock) {
        if (isIQL2Consistent(docFilter, consumer, clock)) {
            return ;
        }
        appendCommentBeforeText(docFilter, sb);

        sb.append('(');
        docFilter.visit(new DocFilter.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(DocFilter.FieldIs fieldIs) {
                sb.append(getText(fieldIs.field)).append('=');
                pp(fieldIs.term);
                return null;
            }

            @Override
            public Void visit(DocFilter.FieldIsnt fieldIsnt) {
                sb.append(getText(fieldIsnt.field)).append("!=");
                pp(fieldIsnt.term);
                return null;
            }

            @Override
            public Void visit(DocFilter.FieldInQuery fieldInQuery) {
                // TODO: do something about fieldInQuery.field.scope
                throw new UnsupportedOperationException("Don't know how to handle FieldInQuery yet");
//                sb.append(fieldName(fieldInQuery.field.field));
//                if (fieldInQuery.isNegated) {
//                    sb.append(" NOT");
//                }
//                sb.append(" IN (");
//                pp(fieldInQuery.query);
//                sb.append(')');
//                return null;
            }

            @Override
            public Void visit(final DocFilter.Between between) {
                pp(between.forMetric(new DocMetric.Field(between.field)), consumer, clock);
                return null;
            }

            private Void inequality(DocFilter.MetricBinop binop, String cmpOp) {
                pp(binop.m1, consumer, clock);
                sb.append(cmpOp);
                pp(binop.m2, consumer, clock);
                return null;
            }

            @Override
            public Void visit(DocFilter.MetricEqual metricEqual) {
                return inequality(metricEqual, "=");
            }

            @Override
            public Void visit(DocFilter.MetricNotEqual metricNotEqual) {
                return inequality(metricNotEqual, "!=");
            }

            @Override
            public Void visit(DocFilter.MetricGt metricGt) {
                return inequality(metricGt, ">");
            }

            @Override
            public Void visit(DocFilter.MetricGte metricGte) {
                return inequality(metricGte, ">=");
            }

            @Override
            public Void visit(DocFilter.MetricLt metricLt) {
                return inequality(metricLt, "<");
            }

            @Override
            public Void visit(DocFilter.MetricLte metricLte) {
                return inequality(metricLte, "<=");
            }

            @Override
            public Void visit(final DocFilter.And and) {
                return visit(and, "and");
            }

            @Override
            public Void visit(final DocFilter.Or or) {
                return visit(or, "or");
            }

            @Override
            public Void visit(DocFilter.Not not) {
                sb.append("not");
                pp(not.filter, consumer, clock);
                return null;
            }

            @Override
            public Void visit(DocFilter.Regex regex) {
                sb.append(getText(regex.field)).append("=~");
                sb.append('"').append(regexEscape(regex.regex)).append('"');
                return null;
            }

            @Override
            public Void visit(DocFilter.NotRegex notRegex) {
                sb.append(getText(notRegex.field)).append("!=~");
                sb.append('"').append(regexEscape(notRegex.regex)).append('"');
                return null;
            }

            @Override
            public Void visit(DocFilter.FieldEqual fieldEqual) {
                sb.append(getText(fieldEqual.field1) + "=" + getText(fieldEqual.field2));
                return null;
            }

            @Override
            public Void visit(DocFilter.Qualified qualified) {
                throw new UnsupportedOperationException("Can't pretty-print qualified things yet: " + qualified);
            }

            @Override
            public Void visit(DocFilter.Lucene lucene) {
                sb.append("lucene(\"").append(stringEscape(lucene.query)).append("\")");
                return null;
            }

            @Override
            public Void visit(DocFilter.Sample sample) {
                sb.append("sample(")
                        .append(getText(sample.field)).append(", ")
                        .append(sample.numerator).append(", ")
                        .append(sample.denominator).append(", ")
                        .append(sample.seed)
                        .append(")");
                return null;
            }

            @Override
            public Void visit(final DocFilter.SampleDocMetric sample) throws RuntimeException {
                sb.append("sample(");
                pp(sample.metric, consumer, clock);
                sb.append(sample.numerator).append(", ")
                        .append(sample.denominator).append(", ")
                        .append(sample.seed)
                        .append(")");
                return null;
            }

            @Override
            public Void visit(DocFilter.Always always) {
                sb.append("true");
                return null;
            }

            @Override
            public Void visit(DocFilter.Never never) {
                sb.append("false");
                return null;
            }

            @Override
            public Void visit(final DocFilter.ExplainFieldIn explainFieldIn) throws RuntimeException {
                throw new UnsupportedOperationException("Can't pretty-print ExplainFieldIn things: " + explainFieldIn);
            }

            @Override
            public Void visit(DocFilter.StringFieldIn stringFieldIn) {
                sb.append(getText(stringFieldIn.field)).append(" IN (");
                final List<String> sortedTerms = Lists.newArrayList(stringFieldIn.terms);
                Collections.sort(sortedTerms);
                boolean first = true;
                for (final String term : sortedTerms) {
                    if (!first) {
                        sb.append(", ");
                    }
                    first = false;
                    sb.append('"').append(stringEscape(term)).append('"');
                }
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocFilter.IntFieldIn intFieldIn) {
                sb.append(getText(intFieldIn.field)).append(" IN (");
                final List<Long> sortedTerms = Lists.newArrayList(intFieldIn.terms);
                Collections.sort(sortedTerms);
                boolean first = true;
                for (final long term : sortedTerms) {
                    if (!first) {
                        sb.append(", ");
                    }
                    first = false;
                    sb.append(term);
                }
                sb.append(')');
                return null;
            }

            private Void visit(final DocFilter.Multiple multiple, final String op) {
                for (int i = 0; i < multiple.filters.size(); i++) {
                    if (i > 0) {
                        sb.append(' ').append(op).append(' ');
                    }
                    pp(multiple.filters.get(i), consumer, clock);
                }
                return null;
            }
        });

        sb.append(')');

        appendCommentAfterText(docFilter, sb);
    }

    private void pp(DocMetric docMetric, Consumer<String> consumer, WallClock clock) {
        if (isIQL2Consistent(docMetric, consumer, clock)) {
            return ;
        }

        appendCommentBeforeText(docMetric, sb);

        docMetric.visit(new DocMetric.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(DocMetric.Log log) {
                sb.append("log(");
                pp(log.metric, consumer, clock);
                sb.append(", ").append(log.scaleFactor);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.PerDatasetDocMetric perDatasetDocMetric) {
                throw new UnsupportedOperationException("Can't pretty print PerDatasetDocMetric: " + perDatasetDocMetric);
            }

            @Override
            public Void visit(DocMetric.Count count) {
                sb.append("count()");
                return null;
            }

            @Override
            public Void visit(final DocMetric.DocId count) throws RuntimeException {
                sb.append("docId()");
                return null;
            }

            @Override
            public Void visit(DocMetric.Field field) {
                sb.append(getText(field));
                return null;
            }

            @Override
            public Void visit(DocMetric.Exponentiate exponentiate) {
                sb.append("exp(");
                pp(exponentiate.metric, consumer, clock);
                sb.append(", ").append(exponentiate.scaleFactor);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Negate negate) {
                sb.append('-');
                sb.append('(');
                pp(negate.m1, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Abs abs) {
                sb.append("abs(");
                pp(abs.m1, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Signum signum) {
                sb.append("signum(");
                pp(signum.m1, consumer, clock);
                sb.append(')');
                return null;
            }

            private Void binop(DocMetric.Binop binop, String op) {
                sb.append('(');
                pp(binop.m1, consumer, clock);
                sb.append(' ').append(op).append(' ');
                pp(binop.m2, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(final DocMetric.Add add) {
                sb.append('(');
                for (int i = 0; i < add.metrics.size(); i++) {
                    if (i > 0) {
                        sb.append(" + ");
                    }
                    pp(add.metrics.get(i), consumer, clock);
                }
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Subtract subtract) {
                return binop(subtract, "-");
            }

            @Override
            public Void visit(DocMetric.Multiply multiply) {
                return binop(multiply, "*");
            }

            @Override
            public Void visit(DocMetric.Divide divide) {
                return binop(divide, "/");
            }

            @Override
            public Void visit(DocMetric.Modulus modulus) {
                return binop(modulus, "%");
            }

            @Override
            public Void visit(DocMetric.Min min) {
                sb.append("min(");
                pp(min.m1, consumer, clock);
                sb.append(", ");
                pp(min.m2, consumer, clock);
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.Max max) {
                sb.append("max(");
                pp(max.m1, consumer, clock);
                sb.append(", ");
                pp(max.m2, consumer, clock);
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.MetricEqual metricEqual) {
                return binop(metricEqual, "=");
            }

            @Override
            public Void visit(DocMetric.MetricNotEqual metricNotEqual) {
                return binop(metricNotEqual, "!=");
            }

            @Override
            public Void visit(DocMetric.MetricLt metricLt) {
                return binop(metricLt, "<");
            }

            @Override
            public Void visit(DocMetric.MetricLte metricLte) {
                return binop(metricLte, "<=");
            }

            @Override
            public Void visit(DocMetric.MetricGt metricGt) {
                return binop(metricGt, ">");
            }

            @Override
            public Void visit(DocMetric.MetricGte metricGte) {
                return binop(metricGte, ">=");
            }

            @Override
            public Void visit(DocMetric.RegexMetric regexMetric) {
                sb.append(getText(regexMetric.field)).append("=~").append(regexEscape(regexMetric.regex));
                return null;
            }

            @Override
            public Void visit(DocMetric.FloatScale floatScale) {
                sb.append("floatscale(")
                    .append(getText(floatScale.field)).append(", ")
                    .append(floatScale.mult).append(", ")
                    .append(floatScale.add)
                    .append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Constant constant) {
                sb.append(constant.value);
                return null;
            }

            @Override
            public Void visit(DocMetric.HasIntField hasIntField) {
                sb.append("hasintfield(").append(getText(hasIntField.field)).append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.HasStringField hasStringField) {
                sb.append("hasstrfield(").append(getText(hasStringField.field)).append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.IntTermCount intTermCount) {
                sb.append("inttermcount(").append(getText(intTermCount.field)).append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.StrTermCount strTermCount) {
                sb.append("strtermcount(").append(getText(strTermCount.field)).append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.HasInt hasInt) {
                sb.append(getText(hasInt.field)).append('=').append(hasInt.term);
                return null;
            }

            @Override
            public Void visit(DocMetric.HasString hasString) {
                sb.append(getText(hasString.field)).append("=\"").append(stringEscape(hasString.term)).append('"');
                return null;
            }

            @Override
            public Void visit(DocMetric.IfThenElse ifThenElse) {
                sb.append('(');
                sb.append("if ");
                pp(ifThenElse.condition, consumer, clock);
                sb.append(" then ");
                pp(ifThenElse.trueCase, consumer, clock);
                sb.append(" else ");
                pp(ifThenElse.falseCase, consumer, clock);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Qualified qualified) {
                throw new UnsupportedOperationException("Can't pretty print qualified things yet: " + qualified);
            }

            @Override
            public Void visit(DocMetric.Extract extract) {
                sb.append("extract(")
                    .append(getText(extract.field)).append(", ")
                    .append(regexEscape(extract.regex)).append(", ")
                    .append(extract.groupNumber)
                  .append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Lucene lucene) throws RuntimeException {
                sb.append("lucene(\"")
                        .append(stringEscape(lucene.query))
                        .append("\")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.FieldEqualMetric equalMetric) throws RuntimeException {
                sb.append(getText(equalMetric.field1)).append("=").append(getText(equalMetric.field2));
                return null;
            }

            @Override
            public Void visit(DocMetric.StringLen stringLen) {
                sb.append("len(").append(getText(stringLen.field)).append(")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.Sample random) throws RuntimeException {
                sb.append("sample")
                        .append('(')
                        .append(getText(random.field))
                        .append(", ")
                        .append(random.numerator)
                        .append(", ")
                        .append(random.denominator)
                        .append(", \"")
                        .append(random.salt)
                        .append("\")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.SampleMetric random) throws RuntimeException {
                sb.append("sample(");
                pp(random.metric, consumer, clock);
                sb.append(", ")
                    .append(random.numerator)
                    .append(", ")
                    .append(random.denominator)
                    .append(", \"")
                    .append(random.salt)
                    .append("\")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.Random random) throws RuntimeException {
                sb.append("random")
                    .append('(')
                    .append(getText(random.field))
                    .append(", ")
                    .append(random.max)
                    .append(", \"")
                    .append(random.salt)
                    .append("\")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.RandomMetric random) throws RuntimeException {
                sb.append("random(");
                pp(random.metric, consumer, clock);
                sb.append(", ")
                    .append(random.max)
                    .append(", \"")
                    .append(random.salt)
                    .append("\")");
                return null;
            }
        });

        appendCommentAfterText(docMetric, sb);
    }

    @VisibleForTesting
    static String stringEscape(String query) {
        return StringEscapeUtils.escapeJava(query);
    }

    @VisibleForTesting
    static String regexEscape(String regex) {
        // TODO: Use different logic if ParserCommon.unquote ever stops being used for regexes.
        return stringEscape(regex);
    }

    private void pp(Term term) {
        if (term.isIntTerm) {
            sb.append(term.intTerm);
        } else {
            sb.append('"').append(stringEscape(term.stringTerm)).append('"');
        }
    }
}
