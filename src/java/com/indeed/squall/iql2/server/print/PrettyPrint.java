package com.indeed.squall.iql2.server.print;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.Positional;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.Term;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang3.StringEscapeUtils;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrettyPrint {
    private static final Function<String, String> RENDER_STRING = new Function<String, String>() {
        public String apply(String s) {
            return "\"" + stringEscape(s) + "\"";
        }
    };

    public static void main(String[] args) {
        final String pretty = prettyPrint("from jobsearch 2d 1d as blah, mobsearch where foo:\"3\" country:us (oji + ojc) = 10 group by something select somethingElse, distinct(thing)");
        System.out.println("pretty = " + pretty);
    }

    @Nonnull
    public static String prettyPrint(String q) {
        final JQLParser.QueryContext queryContext = Queries.parseQueryContext(q, true);
        final Query query = Query.parseQuery(queryContext, Collections.<String, Set<String>>emptyMap(), Collections.<String, Set<String>>emptyMap(), new Consumer<String>() {
            @Override
            public void accept(String s) {

            }
        }, new StoppedClock());
        return prettyPrint(queryContext, query);
    }

    private static String prettyPrint(JQLParser.QueryContext queryContext, Query query) {
        final PrettyPrint prettyPrint = new PrettyPrint(queryContext);
        prettyPrint.pp(query);
        while (prettyPrint.sb.charAt(prettyPrint.sb.length() - 1) == '\n') {
            prettyPrint.sb.setLength(prettyPrint.sb.length() - 1);
        }
        return prettyPrint.sb.toString();
    }

    private final CharStream inputStream;
    private final StringBuilder sb = new StringBuilder();

    private PrettyPrint(JQLParser.QueryContext queryContext) {
        this.inputStream = queryContext.start.getInputStream();
    }

    private String getText(Positional positional) {
        return inputStream.getText(new Interval(positional.getStart().startIndex, positional.getEnd().stopIndex));
    }

    private void pp(Query query) {
        sb.append("FROM ");
        final boolean multiDataSets = query.datasets.size() > 1;

        DateTime firstDatasetStart = null;
        DateTime firstDatasetEnd = null;

        for (final Dataset dataset : query.datasets) {
            if (multiDataSets) {
                sb.append("\n    ");
            }
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
                sb.append(" AS ").append(getText(dataset.alias.get()));
            }
            if (!dataset.fieldAliases.isEmpty()) {
                sb.append(" ALIASING (");
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
        }
        sb.append('\n');

        if (query.filter.isPresent()) {
            sb.append("WHERE ");
            final List<DocFilter> filters = new ArrayList<>();
            unAnd(query.filter.get(), filters);
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sb.append(' ');
                }
                pp(filters.get(i));
            }
            sb.append('\n');
        }

        if (!query.groupBys.isEmpty()) {
            sb.append("GROUP BY ");
            final boolean isMultiGroupBy = query.groupBys.size() > 1;
            boolean isFirst = true;
            for (final GroupByMaybeHaving groupBy : query.groupBys) {
                if (isFirst && isMultiGroupBy) {
                    sb.append("\n    ");
                } else if (!isFirst) {
                    sb.append("\n  , ");
                }
                isFirst = false;
                pp(groupBy);
            }
            sb.append('\n');
        }

        if (!query.selects.isEmpty()) {
            sb.append("SELECT ");
            final boolean isMultiSelect = query.selects.size() > 1;
            boolean isFirst = true;
            for (final AggregateMetric select : query.selects) {
                if (isFirst && isMultiSelect) {
                    sb.append("\n    ");
                } else if (!isFirst) {
                    sb.append("\n  , ");
                }
                isFirst = false;
                pp(select);
            }
            sb.append('\n');
        }
    }

    private void unAnd(DocFilter filter, List<DocFilter> into) {
        if (filter instanceof DocFilter.And) {
            unAnd(((DocFilter.And) filter).f1, into);
            unAnd(((DocFilter.And) filter).f2, into);
        } else {
            into.add(filter);
        }
    }

    private void pp(GroupByMaybeHaving groupBy) {
        pp(groupBy.groupBy);
        if (groupBy.filter.isPresent()) {
            sb.append(" HAVING ");
            pp(groupBy.filter.get());
        }
    }

    private void pp(final GroupBy gb) {
        gb.visit(new GroupBy.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(GroupBy.GroupByMetric groupByMetric) {
                sb.append("BUCKET(");
                pp(groupByMetric.metric);
                sb.append(", ").append(groupByMetric.min);
                sb.append(", ").append(groupByMetric.max);
                sb.append(", ").append(groupByMetric.interval);
                sb.append(", ").append(groupByMetric.excludeGutters ? "1" : "0");
                sb.append(')');
                if (groupByMetric.withDefault) {
                    sb.append(" WITH DEFAULT");
                }
                return null;
            }

            private void timeFieldAndFormat(Optional<Positioned<String>> field, Optional<String> format) {
                if (field.isPresent() || format.isPresent()) {
                    if (format.isPresent()) {
                        sb.append(", ").append('"').append(stringEscape(format.get())).append('"');
                    } else {
                        sb.append(", DEFAULT");
                    }
                    if (field.isPresent()) {
                        sb.append(", ").append(getText(field.get()));
                    }
                }
            }

            @Override
            public Void visit(GroupBy.GroupByTime groupByTime) {
                sb.append("TIME(");
                // TODO: this needs serious improvement
                sb.append(groupByTime.periodMillis / 1000).append('s');
                timeFieldAndFormat(groupByTime.field, groupByTime.format);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByTimeBuckets groupByTimeBuckets) {
                sb.append("TIME(");
                sb.append(groupByTimeBuckets.numBuckets).append('b');
                timeFieldAndFormat(groupByTimeBuckets.field, groupByTimeBuckets.format);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByMonth groupByMonth) {
                sb.append("TIME(1M");
                timeFieldAndFormat(groupByMonth.field, groupByMonth.format);
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
            public Void visit(GroupBy.GroupByField groupByField) {
                sb.append(getText(groupByField.field));
                if (groupByField.metric.isPresent() || groupByField.limit.isPresent()) {
                    sb.append('[');
                    if (groupByField.limit.isPresent()) {
                        sb.append(groupByField.limit.get());
                    }
                    if (groupByField.metric.isPresent()) {
                        sb.append(" BY ");
                        pp(groupByField.metric.get());
                    }
                    if (groupByField.filter.isPresent()) {
                        sb.append(" HAVING ");
                        pp(groupByField.filter.get());
                    }
                    sb.append(']');
                }
                if (groupByField.forceNonStreaming) {
                    sb.append('*');
                }
                if (groupByField.withDefault) {
                    sb.append(" WITH DEFAULT");
                }
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByDayOfWeek groupByDayOfWeek) {
                sb.append("DAYOFWEEK");
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupBySessionName groupBySessionName) {
                throw new UnsupportedOperationException("There is currently no way to group by session name");
            }

            @Override
            public Void visit(GroupBy.GroupByQuantiles groupByQuantiles) {
                sb.append("QUANTILES(").append(getText(groupByQuantiles.field)).append(", ").append(groupByQuantiles.numBuckets).append(")");
                return null;
            }

            @Override
            public Void visit(GroupBy.GroupByPredicate groupByPredicate) {
                pp(groupByPredicate.docFilter);
                return null;
            }
        });
    }

    private void pp(AggregateFilter aggregateFilter) {
        aggregateFilter.visit(new AggregateFilter.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(AggregateFilter.TermIs termIs) {
                sb.append("TERM()=");
                pp(termIs.term);
                throw new UnsupportedOperationException("You need to implement this");
            }

            private Void binop(AggregateMetric m1, String op, AggregateMetric m2) {
                sb.append('(');
                pp(m1);
                sb.append(')');
                sb.append(op);
                sb.append('(');
                pp(m2);
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
            public Void visit(AggregateFilter.And and) {
                sb.append('(');
                pp(and.f1);
                sb.append(')');
                sb.append(" AND ");
                sb.append('(');
                pp(and.f2);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Or or) {
                sb.append('(');
                pp(or.f1);
                sb.append(')');
                sb.append(" OR ");
                sb.append('(');
                pp(or.f2);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Not not) {
                sb.append("NOT(");
                pp(not.filter);
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
                sb.append("TRUE");
                return null;
            }

            @Override
            public Void visit(AggregateFilter.Never never) {
                sb.append("FALSE");
                return null;
            }

            @Override
            public Void visit(AggregateFilter.IsDefaultGroup isDefaultGroup) {
                throw new UnsupportedOperationException("What even is this operation?: " + isDefaultGroup);
            }
        });
    }

    private void pp(AggregateMetric aggregateMetric) {
        aggregateMetric.visit(new AggregateMetric.Visitor<Void, RuntimeException>() {
            private Void binop(AggregateMetric.Binop binop, String op) {
                sb.append('(');
                pp(binop.m1);
                sb.append(')');
                sb.append(op);
                sb.append('(');
                pp(binop.m2);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Add add) {
                return binop(add, "+");
            }

            @Override
            public Void visit(AggregateMetric.Log log) {
                sb.append("LOG(");
                pp(log.m1);
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Negate negate) {
                sb.append('-');
                sb.append('(');
                pp(negate.m1);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Abs abs) {
                sb.append("ABS(");
                pp(abs.m1);
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
                sb.append("PARENT(");
                pp(parent.metric);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Lag lag) {
                sb.append("LAG(");
                sb.append(lag.lag);
                sb.append(", ");
                pp(lag.metric);
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
                sb.append("WINDOW(");
                sb.append(window.window);
                sb.append(", ");
                pp(window.metric);
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
                pp(docStats.metric);
                sb.append(']');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.ImplicitDocStats implicitDocStats) {
                pp(implicitDocStats.docMetric);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Constant constant) {
                sb.append(constant.value);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Percentile percentile) {
                sb.append("PERCENTILE(");
                sb.append(getText(percentile.field));
                sb.append(", ");
                sb.append(percentile.percentile);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Running running) {
                sb.append("RUNNING(");
                pp(running.metric);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Distinct distinct) {
                sb.append("DISTINCT(");
                sb.append(getText(distinct.field));
                if (distinct.filter.isPresent()) {
                    sb.append(" HAVING ");
                    pp(distinct.filter.get());
                }
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Named named) {
                sb.append('(');
                pp(named.metric);
                sb.append(')');
                sb.append(" AS ");
                sb.append(getText(named.name));
                return null;
            }

            @Override
            public Void visit(AggregateMetric.GroupStatsLookup groupStatsLookup) {
                throw new UnsupportedOperationException("Shouldn't be rendering this");
            }

            @Override
            public Void visit(AggregateMetric.GroupStatsMultiLookup groupStatsMultiLookup) throws RuntimeException {
                throw new UnsupportedOperationException("Shouldn't be rendering this");
            }

            @Override
            public Void visit(AggregateMetric.SumAcross sumAcross) {
                sb.append("SUM_OVER(");
                pp(sumAcross.groupBy);
                sb.append(", ");
                pp(sumAcross.metric);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.IfThenElse ifThenElse) {
                sb.append("IF ");
                pp(ifThenElse.condition);
                sb.append(" THEN ");
                pp(ifThenElse.trueCase);
                sb.append(" ELSE ");
                pp(ifThenElse.falseCase);
                return null;
            }

            @Override
            public Void visit(AggregateMetric.FieldMin fieldMin) {
                sb.append("FIELD_MIN(");
                sb.append(getText(fieldMin.field));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.FieldMax fieldMax) {
                sb.append("FIELD_MAX(");
                sb.append(getText(fieldMax.field));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Min min) {
                sb.append("MIN(");
                Joiner.on(", ").appendTo(sb, Iterables.transform(min.metrics, new Function<AggregateMetric, String>() {
                    public String apply(AggregateMetric aggregateMetric) {
                        final StringBuilder sb = new StringBuilder();
                        pp(aggregateMetric);
                        return sb.toString();
                    }
                }));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Max max) {
                sb.append("MAX(");
                Joiner.on(", ").appendTo(sb, Iterables.transform(max.metrics, new Function<AggregateMetric, String>() {
                    public String apply(AggregateMetric aggregateMetric) {
                        final StringBuilder sb = new StringBuilder();
                        pp(aggregateMetric);
                        return sb.toString();
                    }
                }));
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(AggregateMetric.Bootstrap bootstrap) throws RuntimeException {
                throw new UnsupportedOperationException("You need to implement this");
            }
        });
    }

    private void pp(DocFilter docFilter) {
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
            public Void visit(DocFilter.Between between) {
                pp(new DocFilter.And(
                        new DocFilter.MetricGte(new DocMetric.Constant(between.lowerBound), new DocMetric.Field(between.field)),
                        new DocFilter.MetricLt(new DocMetric.Field(between.field), new DocMetric.Constant(between.upperBound))
                ));
                return null;
            }

            private Void inequality(DocFilter.MetricBinop binop, String cmpOp) {
                pp(binop.m1);
                sb.append(cmpOp);
                pp(binop.m2);
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
            public Void visit(DocFilter.And and) {
                pp(and.f1);
                sb.append(" AND ");
                pp(and.f2);
                return null;
            }

            @Override
            public Void visit(DocFilter.Or or) {
                pp(or.f1);
                sb.append(" OR ");
                pp(or.f2);
                return null;
            }

            @Override
            public Void visit(DocFilter.Ors ors) {
                if (ors.filters.isEmpty()) {
                    pp(new DocFilter.Never());
                } else {
                    boolean first = true;
                    for (final DocFilter filter : ors.filters) {
                        if (!first) {
                            sb.append(" OR ");
                        }
                        first = false;
                        pp(filter);
                    }
                }
                return null;
            }

            @Override
            public Void visit(DocFilter.Not not) {
                sb.append("NOT");
                pp(not.filter);
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
                sb.append("LUCENE(\"").append(stringEscape(lucene.query)).append("\")");
                return null;
            }

            @Override
            public Void visit(DocFilter.Sample sample) {
                sb.append("SAMPLE(")
                        .append(getText(sample.field)).append(", ")
                        .append(sample.numerator).append(", ")
                        .append(sample.denominator).append(", ")
                        .append(sample.seed)
                        .append(")");
                return null;
            }

            @Override
            public Void visit(DocFilter.Always always) {
                sb.append("TRUE");
                return null;
            }

            @Override
            public Void visit(DocFilter.Never never) {
                sb.append("FALSE");
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
        });

        sb.append(')');
    }

    private void pp(DocMetric docMetric) {
        docMetric.visit(new DocMetric.Visitor<Void, RuntimeException>() {
            @Override
            public Void visit(DocMetric.Log log) {
                sb.append("LOG(");
                pp(log.metric);
                sb.append(", ").append(log.scaleFactor);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.PushableDocMetric pushableDocMetric) {
                throw new UnsupportedOperationException("uhhh");
            }

            @Override
            public Void visit(DocMetric.Count count) {
                sb.append("COUNT()");
                return null;
            }

            @Override
            public Void visit(DocMetric.Field field) {
                sb.append(getText(field));
                return null;
            }

            @Override
            public Void visit(DocMetric.Exponentiate exponentiate) {
                sb.append("EXP(");
                pp(exponentiate.metric);
                sb.append(", ").append(exponentiate.scaleFactor);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Negate negate) {
                sb.append('-');
                sb.append('(');
                pp(negate.m1);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Abs abs) {
                sb.append("ABS(");
                pp(abs.m1);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Signum signum) {
                sb.append("SIGNUM(");
                pp(signum.m1);
                sb.append(')');
                return null;
            }

            private Void binop(DocMetric.Binop binop, String op) {
                sb.append('(');
                pp(binop.m1);
                sb.append(' ').append(op).append(' ');
                pp(binop.m2);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Add add) {
                return binop(add, "+");
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
                sb.append("MIN(");
                pp(min.m1);
                sb.append(", ");
                pp(min.m2);
                sb.append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.Max max) {
                sb.append("MAX(");
                pp(max.m1);
                sb.append(", ");
                pp(max.m2);
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
                sb.append("FLOATSCALE(")
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
                sb.append("HASINTFIELD(").append(getText(hasIntField.field)).append(")");
                return null;
            }

            @Override
            public Void visit(DocMetric.HasStringField hasStringField) {
                sb.append("HASSTRFIELD(").append(getText(hasStringField.field)).append(")");
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
                sb.append("IF ");
                pp(ifThenElse.condition);
                sb.append(" THEN ");
                pp(ifThenElse.trueCase);
                sb.append(" ELSE ");
                pp(ifThenElse.falseCase);
                sb.append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Qualified qualified) {
                throw new UnsupportedOperationException("Can't pretty print qualified things yet: " + qualified);
            }

            @Override
            public Void visit(DocMetric.Extract extract) {
                sb.append("EXTRACT(")
                    .append(getText(extract.field)).append(", ")
                    .append(regexEscape(extract.regex)).append(", ")
                    .append(extract.groupNumber)
                  .append(')');
                return null;
            }

            @Override
            public Void visit(DocMetric.Lucene lucene) throws RuntimeException {
                sb.append("LUCENE(\"")
                        .append(stringEscape(lucene.query))
                        .append("\")");
                return null;
            }

            @Override
            public Void visit(final DocMetric.FieldEqualMetric equalMetric) throws RuntimeException {
                sb.append(getText(equalMetric.field1)).append("=").append(getText(equalMetric.field2));
                return null;
            }
        });
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
