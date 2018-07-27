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
 package com.indeed.imhotep.sql;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.RegexTooComplexException;
import com.indeed.imhotep.ez.DynamicMetric;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.iql.Condition;
import com.indeed.imhotep.iql.DiffGrouping;
import com.indeed.imhotep.iql.DistinctGrouping;
import com.indeed.imhotep.iql.FieldGrouping;
import com.indeed.imhotep.iql.Grouping;
import com.indeed.imhotep.iql.IQLQuery;
import com.indeed.imhotep.iql.IntInCondition;
import com.indeed.imhotep.iql.MetricCondition;
import com.indeed.imhotep.iql.PercentileGrouping;
import com.indeed.imhotep.iql.QueryCondition;
import com.indeed.imhotep.iql.RegexCondition;
import com.indeed.imhotep.iql.SampleCondition;
import com.indeed.imhotep.iql.StatRangeGrouping;
import com.indeed.imhotep.iql.StatRangeGrouping2D;
import com.indeed.imhotep.iql.StringInCondition;
import com.indeed.imhotep.metadata.DatasetMetadata;
import com.indeed.imhotep.metadata.FieldMetadata;
import com.indeed.imhotep.sql.ast.BinaryExpression;
import com.indeed.imhotep.sql.ast.Expression;
import com.indeed.imhotep.sql.ast.FunctionExpression;
import com.indeed.imhotep.sql.ast.NameExpression;
import com.indeed.imhotep.sql.ast.NumberExpression;
import com.indeed.imhotep.sql.ast.Op;
import com.indeed.imhotep.sql.ast.StringExpression;
import com.indeed.imhotep.sql.ast.TupleExpression;
import com.indeed.imhotep.sql.ast2.FromClause;
import com.indeed.imhotep.sql.ast2.SelectStatement;
import com.indeed.imhotep.sql.parser.ExpressionParser;
import com.indeed.imhotep.sql.parser.PeriodParser;
import com.indeed.imhotep.web.ImhotepMetadataCache;
import com.indeed.imhotep.web.Limits;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.util.serialization.LongStringifier;
import com.indeed.util.serialization.Stringifier;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.indeed.imhotep.ez.EZImhotepSession.abs;
import static com.indeed.imhotep.ez.EZImhotepSession.add;
import static com.indeed.imhotep.ez.EZImhotepSession.aggDiv;
import static com.indeed.imhotep.ez.EZImhotepSession.aggDivConst;
import static com.indeed.imhotep.ez.EZImhotepSession.cached;
import static com.indeed.imhotep.ez.EZImhotepSession.constant;
import static com.indeed.imhotep.ez.EZImhotepSession.counts;
import static com.indeed.imhotep.ez.EZImhotepSession.div;
import static com.indeed.imhotep.ez.EZImhotepSession.dynamic;
import static com.indeed.imhotep.ez.EZImhotepSession.exp;
import static com.indeed.imhotep.ez.EZImhotepSession.floatScale;
import static com.indeed.imhotep.ez.EZImhotepSession.greater;
import static com.indeed.imhotep.ez.EZImhotepSession.greaterEq;
import static com.indeed.imhotep.ez.EZImhotepSession.hasInt;
import static com.indeed.imhotep.ez.EZImhotepSession.hasIntField;
import static com.indeed.imhotep.ez.EZImhotepSession.hasString;
import static com.indeed.imhotep.ez.EZImhotepSession.hasStringField;
import static com.indeed.imhotep.ez.EZImhotepSession.intField;
import static com.indeed.imhotep.ez.EZImhotepSession.isEqual;
import static com.indeed.imhotep.ez.EZImhotepSession.isNotEqual;
import static com.indeed.imhotep.ez.EZImhotepSession.less;
import static com.indeed.imhotep.ez.EZImhotepSession.lessEq;
import static com.indeed.imhotep.ez.EZImhotepSession.log;
import static com.indeed.imhotep.ez.EZImhotepSession.lucene;
import static com.indeed.imhotep.ez.EZImhotepSession.max;
import static com.indeed.imhotep.ez.EZImhotepSession.min;
import static com.indeed.imhotep.ez.EZImhotepSession.mod;
import static com.indeed.imhotep.ez.EZImhotepSession.mult;
import static com.indeed.imhotep.ez.EZImhotepSession.multiplyShiftRight;
import static com.indeed.imhotep.ez.EZImhotepSession.shiftLeftDivide;
import static com.indeed.imhotep.ez.EZImhotepSession.sub;
import static com.indeed.imhotep.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class IQLTranslator {
    private static final Logger log = Logger.getLogger(IQLTranslator.class);

    public static IQLQuery translate(SelectStatement parse, ImhotepClient client, String username, ImhotepMetadataCache metadata,
                                     Limits limits) {
        if(log.isTraceEnabled()) {
            log.trace(parse.toHashKeyString());
        }

        final FromClause fromClause = parse.from;
        final String dataset = fromClause.getDataset();
        final DatasetMetadata datasetMetadata = metadata.getDataset(dataset);
        final List<Stat> stats = Lists.newArrayList();

        final Set<String> fieldNames = Sets.newHashSet();
        final List<Expression> projections = Lists.newArrayList(parse.select.getProjections());
        final DistinctGrouping distinctGrouping = getDistinctGrouping(projections, datasetMetadata, fieldNames);
        final PercentileGrouping percentileGrouping = getPercentileGrouping(projections, datasetMetadata, EZImhotepSession.counts(), fieldNames);

        if (distinctGrouping != null && percentileGrouping != null) {
            throw new IllegalArgumentException("Cannot use distinct and percentile in the same query");
        }

        final StatMatcher statMatcher = new StatMatcher(datasetMetadata, fieldNames);
        for (Expression expression : projections) {
            stats.add(expression.match(statMatcher));
        }

        final List<Condition> conditions;
        if (parse.where != null) {
            conditions = parse.where.getExpression().match(new ConditionMatcher(datasetMetadata, fieldNames));
        } else {
            conditions = Collections.emptyList();
        }

        final List<Grouping> groupings = Lists.newArrayList();
        final GroupByMatcher groupByMatcher = new GroupByMatcher(datasetMetadata, fromClause.getStart(), fromClause.getEnd(), parse.limit, limits, fieldNames);
        if (parse.groupBy != null) {
            for (Expression groupBy : parse.groupBy.groupings) {
                groupings.add(groupBy.match(groupByMatcher));
            }
        }

        if(distinctGrouping != null) {
            ensureDistinctSelectDoesntMatchGroupings(groupings, distinctGrouping);
            groupings.add(distinctGrouping);    // distinct has to come last
        } else if (percentileGrouping != null) {
            groupings.add(percentileGrouping);
        }

        handleMultitermIn(conditions, groupings, limits);

        handleDiffGrouping(groupings, stats, limits);

        optimizeGroupings(groupings, limits);

        return new IQLQuery(client, stats, fromClause.getDataset(), fromClause.getStart(), fromClause.getEnd(),
                conditions, groupings, parse.limit, username, metadata, limits, fieldNames);
    }

    private static void ensureDistinctSelectDoesntMatchGroupings(List<Grouping> groupings, DistinctGrouping distinctGrouping) {
        for(Field distinctField : distinctGrouping.getFields()) {
            for(Grouping grouping: groupings) {
                if(grouping instanceof FieldGrouping && ((FieldGrouping) grouping).getField().equals(distinctField)) {
                    throw new IqlKnownException.ParseErrorException("Please remove distinct(" + distinctField.getFieldName() +
                        ") from the SELECT clause as it is always going to be 1 due to it being one of the GROUP BY groups");
                }
            }
        }
    }

    private static void optimizeGroupings(List<Grouping> groupings, Limits limits) {
        // if we have only one grouping we can safely disable exploding which allows us to stream the result
        if(groupings.size() == 1 && groupings.get(0) instanceof FieldGrouping) {
            FieldGrouping fieldGrouping = (FieldGrouping) groupings.get(0);
            if(!fieldGrouping.isNoExplode()
                    && !fieldGrouping.isTopK()
                    && !fieldGrouping.isTermSubset()) {
                groupings.set(0, new FieldGrouping(fieldGrouping.getField(), true, fieldGrouping.getRowLimit(), limits));
            }
        }
    }

    /**
     * Handles converting queries of the form WHERE field IN (term1, term2, ...) GROUP BY field
     * to queries like: GROUP BY field IN (term1, term2, ...) .
     * This properly handles the case where filtered and grouped by field has multiple terms per doc (e.g. grp, rcv).
     * Modifies the passed in lists.
     */
    static void handleMultitermIn(List<Condition> conditions, List<Grouping> groupings, Limits limits) {
        for(int i = 0; i < conditions.size(); i++) {
            Condition condition = conditions.get(i);
            if(! (condition instanceof StringInCondition)) {
                continue;
            }
            StringInCondition inCondition = (StringInCondition) condition;
            if(inCondition.isEquality()) {
                continue;   // when we have a single value filter (e.g. grp:smartphone), assume that MultitermIn is not intended
            }
            if(inCondition.isNegation()) {
                continue;   // negations shouldn't be rewritten
            }
            Field.StringField field = inCondition.getStringField();
            // see if this field is also used in GROUP BY
            for(int j = 0; j < groupings.size(); j++) {
                Grouping grouping = groupings.get(j);
                if(!(grouping instanceof FieldGrouping)) {
                    continue;
                }
                FieldGrouping fieldGrouping = (FieldGrouping) grouping;
                if(!field.equals(fieldGrouping.getField()) || fieldGrouping.isTopK()) {
                    continue;
                }
                // got a match. convert this grouping to a FieldInGrouping and remove the condition
                FieldGrouping fieldInGrouping = new FieldGrouping(field, fieldGrouping.isNoExplode(),
                        Lists.newArrayList(inCondition.getValues()), limits);
                conditions.remove(i);
                i--;    // have to redo the current index as indexes were shifted
                groupings.set(j, fieldInGrouping);
            }
        }
    }

    /**
     * Handles converting queries of the form GROUP BY diff(field, filter1, filter2, limit) SELECT metric
     * to queries like: GROUP BY field[top limit by abs(filter1*metric-filter2*metric)] select abs(filter1*metric-filter2*metric), filter1*metric, filter2*metric.
     * This properly handles the case where filtered and grouped by field has multiple terms per doc (e.g. grp, rcv).
     * Modifies the passed in lists.
     */
    private static void handleDiffGrouping(List<Grouping> groupings, List<Stat> stats, Limits limits) {
        for(int i = 0; i < groupings.size(); i++) {
            final Grouping grouping = groupings.get(i);
            if(!(grouping instanceof DiffGrouping)) {
                continue;
            }
            final Stat selectStat= stats.get(0);

            DiffGrouping diff = (DiffGrouping) grouping;
            Stat filter1 = diff.getFilter1();
            Stat filter2 = diff.getFilter2();

            Stat stat1 = mult(filter1, selectStat);
            Stat stat2 = mult(filter2, selectStat);

            Stat diffStat = abs(sub(stat1, stat2));
            stats.set(0, diffStat);
            // TODO: make client understand
            if(stats.size() > 1) {
                stats.set(1, stat1);
            } else {
                stats.add(stat1);
            }
            if(stats.size() > 2) {
                stats.set(2, stat2);
            } else {
                stats.add(stat2);
            }
            groupings.set(i, new FieldGrouping(diff.getField(), diff.getTopK(), diffStat, false, limits));
        }
    }

    static DistinctGrouping getDistinctGrouping(List<Expression> projections, DatasetMetadata datasetMetadata, Set<String> fieldNames) {
        DistinctGrouping distinctGrouping = null;
        int projectionNumber = 0;
        for(Iterator<Expression> projectionsIter = projections.iterator(); projectionsIter.hasNext(); projectionNumber++) {
            Expression projection = projectionsIter.next();
            if(!(projection instanceof FunctionExpression)) {
                continue;
            }
            FunctionExpression functionProjection = (FunctionExpression) projection;
            if (!functionProjection.function.equals("distinct")) {
                continue;
            }
            if(functionProjection.args.size() != 1) {
                throw new IqlKnownException.ParseErrorException("distinct() takes a field name as an argument and returns distinct count of terms for the field");
            }

            String fieldName = getStr(functionProjection.args.get(0));
            final Field field = getField(fieldName, datasetMetadata);
            fieldNames.add(fieldName);
            projectionsIter.remove();

            if(distinctGrouping == null) {
                distinctGrouping = new DistinctGrouping();
            }
            distinctGrouping.addField(field, projectionNumber);
        }
        return distinctGrouping;
    }

    static PercentileGrouping getPercentileGrouping(List<Expression> projections, DatasetMetadata datasetMetadata, Stat countStat, Set<String> fieldNames) {
        PercentileGrouping percentileGrouping = null;
        int projectionNumber = 0;
        for(Iterator<Expression> projectionsIter = projections.iterator(); projectionsIter.hasNext(); projectionNumber++) {
            Expression projection = projectionsIter.next();
            if(!(projection instanceof FunctionExpression)) {
                continue;
            }
            FunctionExpression functionProjection = (FunctionExpression) projection;
            if (!functionProjection.function.equals("percentile")) {
                continue;
            }
            if(functionProjection.args.size() != 2) {
                throw new IqlKnownException.ParseErrorException(
                        "percentile() takes a field name and a percentile and returns that percentile, e.g. percentile(tottime, 50)"
                );
            }

            String fieldName = getStr(functionProjection.args.get(0));
            final Field field = getField(fieldName, datasetMetadata);
            fieldNames.add(fieldName);
            projectionsIter.remove();

            final double percentile = parseInt(functionProjection.args.get(1));
            if (percentile < 0 || percentile > 100) {
                throw new IqlKnownException.ParseErrorException("percentile must be between 0 and 100");
            }

            if(percentileGrouping == null) {
                percentileGrouping = new PercentileGrouping(countStat);
            }
            percentileGrouping.addPercentileQuery(field, percentile, projectionNumber);
        }
        return percentileGrouping;
    }

    /**
     * Constructs the right type of Field depending on the available metadata.
     * @throws IqlKnownException.UnknownFieldException if field is not found.
     */
    @Nonnull
    private static Field getField(String name, DatasetMetadata datasetMetadata) {
        final FieldMetadata fieldMetadata = datasetMetadata.getField(name);
        if(fieldMetadata == null) {
            throw new IqlKnownException.UnknownFieldException("Unknown field: " + name);
        }
        return fieldMetadata.isIntImhotepField() ? Field.intField(name) : Field.stringField(name);
    }

    private static class StatMatcher extends Expression.Matcher<Stat> {
        private final DatasetMetadata datasetMetadata;

        private final Map<String, Function<List<Expression>, Stat>> statLookup;

        private final Set<String> fieldNames;

        private Stat[] getStats(final List<Expression> input) {
            List<Stat> stats = Lists.newArrayList();
            for(Expression statString : input) {
                stats.add(statString.match(StatMatcher.this));
            }
            return stats.toArray(new Stat[stats.size()]);
        }

        private StatMatcher(final DatasetMetadata datasetMetadata, final Set<String> fieldNames) {
            this.datasetMetadata = datasetMetadata;
            this.fieldNames = fieldNames;
            final ImmutableMap.Builder<String, Function<List<Expression>, Stat>> builder = ImmutableMap.builder();
            builder.put("count", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if(input.size() > 0) {
                        throw new IqlKnownException.ParseErrorException("Only count() with no arguments is supported which returns the total number of documents in the group");
                    }
                    return counts();
                }
            });
            builder.put("cached", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("cached() requires one argument.");
                    }
                    return cached(input.get(0).match(StatMatcher.this));
                }
            });
            builder.put("abs", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("abc() requires one argument.");
                    }
                    return abs(input.get(0).match(StatMatcher.this));
                }
            });
            builder.put("min", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() < 2) {
                        throw new IqlKnownException.ParseErrorException("min() requires at least 2 arguments. Did you mean FIELD_MIN() function from IQL2?");
                    }
                    return min(getStats(input));
                }
            });
            builder.put("mulshr", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 3) {
                        throw new IqlKnownException.ParseErrorException("mulshr requires 3 arguments: shift, stat1, stat2");
                    }
                    if(!(input.get(0) instanceof NumberExpression)) {
                        throw new IqlKnownException.ParseErrorException("First argument of mulshr() has to be an integer. ");
                    }
                    final String shiftStr = getStr(input.get(0));
                    final int shift = Integer.parseInt(shiftStr);
                    final Stat stat1 = input.get(1).match(StatMatcher.this);
                    final Stat stat2 = input.get(2).match(StatMatcher.this);
                    return multiplyShiftRight(shift, stat1, stat2);
                }
            });
            builder.put("shldiv", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 3) {
                        throw new IqlKnownException.ParseErrorException("shldiv requires 3 arguments: shift, stat1, stat2");
                    }
                    if(!(input.get(0) instanceof NumberExpression)) {
                        throw new IqlKnownException.ParseErrorException("First argument of shldiv() has to be an integer. ");
                    }
                    final String shiftStr = getStr(input.get(0));
                    final int shift = Integer.parseInt(shiftStr);
                    final Stat stat1 = input.get(1).match(StatMatcher.this);
                    final Stat stat2 = input.get(2).match(StatMatcher.this);
                    return shiftLeftDivide(shift, stat1, stat2);
                }
            });
            builder.put("max", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() < 2) {
                        throw new IqlKnownException.ParseErrorException("max() requires at least 2 arguments. Did you mean FIELD_MAX() function from IQL2?");
                    }
                    return max(getStats(input));
                }
            });
            builder.put("exp", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    int scaleFactor = 1;
                    if(input.size() == 2) {
                        scaleFactor = parseInt(input.get(1));
                    } else if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("exp() requires 1 or 2 arguments. " +
                                "e.g. exp(ojc, 1) where ojc is a metric and 1 is a scaling factor that the terms " +
                                "get divided by before exponentiation and multiplied by after exponentiation. " +
                                "Scaling factor defaults to 1.");
                    }
                    return exp(input.get(0).match(StatMatcher.this), scaleFactor);
                }
            });
            builder.put("log", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    int scaleFactor = 1;
                    if(input.size() == 2) {
                        scaleFactor = parseInt(input.get(1));
                    } else if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("log() requires 1 or 2 arguments. " +
                                "e.g. log(ojc, 1) where ojc is a metric and 1 is a scaling factor. " +
                                "The resulting values are as follows: (Math.log(term) - Math.log(scaleFactor)) * scaleFactor. " +
                                "Scaling factor defaults to 1.");
                    }
                    return log(input.get(0).match(StatMatcher.this), scaleFactor);
                }
            });
            builder.put("dynamic", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("dynamic() requires one argument.");
                    }
                    String name = getName(input.get(0));
                    fieldNames.add(name);
                    return dynamic(new DynamicMetric(name));
                }
            });
            builder.put("hasstr", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    String field = null;
                    String value = null;
                    if (input.size() == 1) {
                        final String param = getStr(input.get(0));
                        final String[] parts = param.split(":");
                        if(parts.length == 2) {
                            field = parts[0];
                            value = parts[1];
                        } else if(parts.length == 1 && param.trim().endsWith(":")) {
                            field = parts[0];
                            value = "";
                        }
                    } else if(input.size() == 2) {
                        field = getStr(input.get(0));
                        value = getStr(input.get(1));
                    }

                    if(Strings.isNullOrEmpty(field) || value == null) {
                        throw new IqlKnownException.ParseErrorException("incorrect usage in hasstr(). Examples: hasstr(rcv,jsv) or hasstr(\"rcv:jsv\")");
                    }
                    fieldNames.add(field);
                    return hasString(field, value);
                }
            });
            builder.put("hasint", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    final String usageExamples = "Examples: hasint(clicked,1) or hasint(\"clicked:1\")";
                    String field = null;
                    long value = 0;
                    if (input.size() == 1) {
                        final String param = getStr(input.get(0));
                        final String[] parts = param.split(":");
                        if(parts.length == 2) {
                            field = parts[0];
                            try {
                                value = Long.parseLong(parts[1]);
                            } catch (NumberFormatException ignored) {
                                throw new IqlKnownException.ParseErrorException("Value in hasint() has to be an integer. " + usageExamples);
                            }
                        }
                    } else if(input.size() == 2) {
                        field = getStr(input.get(0));
                        if(!(input.get(1) instanceof NumberExpression)) {
                            throw new IqlKnownException.ParseErrorException("Second argument of hasint() has to be an integer. " + usageExamples);
                        }
                        value = parseLong(input.get(1));
                    }

                    if(Strings.isNullOrEmpty(field)) {
                        throw new IqlKnownException.ParseErrorException("incorrect usage in hasint(). " + usageExamples);
                    }
                    fieldNames.add(field);
                    return hasInt(field, value);
                }
            });
            builder.put("hasstrfield", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("hasstrfield() requires the field name as the argument. Example: hasstrfield(\"rcv\")");
                    }
                    final String field = getStr(input.get(0));
                    if(Strings.isNullOrEmpty(field)) {
                        throw new IqlKnownException.ParseErrorException("incorrect usage in hasstrfield(). Example: hasstrfield(\"rcv\")");
                    }
                    fieldNames.add(field);
                    return hasStringField(field);
                }
            });
            builder.put("hasintfield", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("hasintfield() requires the field name as the argument. Example: hasintfield(\"sjc\")");
                    }
                    final String field = getStr(input.get(0));
                    if(Strings.isNullOrEmpty(field)) {
                        throw new IqlKnownException.ParseErrorException("incorrect usage in hasintfield(). Example: hasintfield(\"sjc\")");
                    }
                    fieldNames.add(field);
                    return hasIntField(field);
                }
            });
            builder.put("floatscale", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() == 3) {
                    	final String name = getName(input.get(0));
                    	fieldNames.add(name);
                    	return floatScale(name, parseLong(input.get(1)), parseLong(input.get(2)));
                    } else if (input.size() == 2) {
                    	final String name = getName(input.get(0));
                    	fieldNames.add(name);
                    	return floatScale(name, parseLong(input.get(1)), 0);
                    } else if(input.size() == 1) {
                    	final String name = getName(input.get(0));
                    	fieldNames.add(name);
                    	return floatScale(name, 1, 0);
                    } else {
                        throw new IqlKnownException.ParseErrorException("floatscale() requires 1, 2, or 3 arguments");
                    }
                }
            });
            builder.put("lucene", new Function<List<Expression>, Stat>() {
                public Stat apply(final List<Expression> input) {
                    if (input.size() != 1) {
                        throw new IqlKnownException.ParseErrorException("lucene() requires a string argument containing the lucene query to try on each document");
                    }
                    final String luceneQuery = getStr(input.get(0));
                    final com.indeed.flamdex.query.Query flamdexQuery = parseLuceneQuery(luceneQuery, datasetMetadata);
                    return lucene(flamdexQuery);
                }
            });
            statLookup = builder.build();
        }

        protected Stat binaryExpression(final Expression left, final Op op, final Expression right) {
            switch (op) {

                case PLUS:
                    return add(left.match(this), right.match(this));
                case MINUS:
                    return sub(left.match(this), right.match(this));
                case MUL:
                    return mult(left.match(this), right.match(this));
                case DIV:
                    return div(left.match(this), right.match(this));
                case MOD:
                    return mod(left.match(this), right.match(this));
                case LESS:
                    return less(left.match(this), right.match(this));
                case LESS_EQ:
                    return lessEq(left.match(this), right.match(this));
                case EQ:
                    if(left instanceof NameExpression && (
                            right instanceof NumberExpression ||
                            right instanceof StringExpression)) {
                        // probably a has[str/int] operation
                        final String fieldName = ((NameExpression) left).name;
                        final FieldMetadata field = datasetMetadata.getField(fieldName);
                        if(field == null) {
                            throw new IqlKnownException.UnknownFieldException("Field not found: " + fieldName);
                        }
                        fieldNames.add(fieldName);
                        if(field.isIntImhotepField() && right instanceof NumberExpression) {
                            long value = parseLong(right);
                            return hasInt(fieldName, value);
                        } else {
                            return hasString(fieldName, getStr(right));
                        }
                        // if it got here, it's not a has[str/int] operation
                    }
                    // try to compare as metrics
                    return isEqual(left.match(this), right.match(this));
                case NOT_EQ:
                    if(left instanceof NameExpression && (
                            right instanceof NumberExpression ||
                            right instanceof StringExpression)) {
                        final Stat equalsStat = binaryExpression(left, Op.EQ, right);
                        // TODO: only return if equalsStat is a HasIntStat or a HasStringStat
                        return sub(counts(), equalsStat);
                    }
                    // try to compare as metrics
                    return isNotEqual(left.match(this), right.match(this));
                case GREATER:
                    return greater(left.match(this), right.match(this));
                case GREATER_EQ:
                    return greaterEq(left.match(this), right.match(this));
                case AGG_DIV:
                {
                    if(right instanceof NumberExpression) {
                        long value = parseLong(right);
                        if(value == 0) {
                            throw new IqlKnownException.ParseErrorException("Can't divide by 0");
                        }
                        return aggDivConst(left.match(this), value);
                    }
                    return aggDiv(left.match(this), right.match(this));
                }
                default:
                    throw new IllegalStateException();
            }
        }

        @Override
        protected Stat unaryExpression(Op op, Expression operand) {
            if(operand instanceof NumberExpression) {
                final String stringValue = "-" + ((NumberExpression)operand).number;
                final long value = Long.parseLong(stringValue);
                return constant(value);
            } else {
                throw new UnsupportedOperationException("Unary negation is only supported on constants");
            }
        }

        protected Stat functionExpression(final String name, final List<Expression> args) {
            final Function<List<Expression>, Stat> function = statLookup.get(name);
            if (function == null) {
                throw new IqlKnownException.ParseErrorException("Unknown stat function: " + name);
            }
            return function.apply(args);
        }

        protected Stat nameExpression(final String name) {
            if(!datasetMetadata.hasField(name)) {
                throw new IqlKnownException.UnknownFieldException("Unknown field name in a stat: " + name);
            }
            fieldNames.add(name);
            return intField(name);
        }

        protected Stat numberExpression(final String value) {
            return constant(Long.parseLong(value));
        }
    }

    private static final class ConditionMatcher extends Expression.Matcher<List<Condition>> {

        private final Map<String, Function<List<Expression>, Condition>> functionLookup;

        private final DatasetMetadata datasetMetadata;

        private final StatMatcher statMatcher;

        private boolean negation; // keeps track of whether we are inside a negated branch of expression

        private final Set<String> fieldNames;

        private ConditionMatcher(final DatasetMetadata datasetMetadata, final Set<String> fieldNames) {
            this.datasetMetadata = datasetMetadata;
            this.fieldNames = fieldNames;
            statMatcher = new StatMatcher(datasetMetadata, fieldNames);
            final ImmutableMap.Builder<String, Function<List<Expression>, Condition>> builder = ImmutableMap.builder();

            Function<List<Expression>, Condition> luceneQueryHandler = new Function<List<Expression>, Condition>() {
                public Condition apply(final List<Expression> input) {
                    if (input.size() != 1) throw new IllegalArgumentException("lucene query function takes exactly one string parameter");
                    final String queryString = getStr(input.get(0));
                    final com.indeed.flamdex.query.Query luceneQuery = parseLuceneQuery(queryString, datasetMetadata);
                    return new QueryCondition(luceneQuery, negation);
                }
            };
            builder.put("lucene", luceneQueryHandler);

            // TODO: remove
            builder.put("query", luceneQueryHandler);
            // TODO: remove. can relax parsing of function params when it's done
            builder.put("between", new Function<List<Expression>, Condition>() {
                public Condition apply(final List<Expression> input) {
                    if (input.size() != 3) throw new IllegalArgumentException("between requires 3 arguments: stat, min, max. " + input.size() + " provided");
                    final Stat stat = input.get(0).match(statMatcher);
                    final long min = parseLong(input.get(1));
                    final long max = parseLong(input.get(2));
                    return new MetricCondition(stat, min, max, negation);
                }
            });
            builder.put("sample", new Function<List<Expression>, Condition>() {
                public Condition apply(final List<Expression> input) {
                    if (input.size() < 2 || input.size() > 4) throw new IllegalArgumentException("sample() requires 2 to 4 arguments: fieldName, samplingRatioNumerator, [samplingRatioDenominator=100], [randomSeed]. " + input.size() + " provided");
                    final Expression arg0 = input.get(0);
                    if(!(arg0 instanceof NameExpression)) {
                        throw new IqlKnownException.ParseErrorException("sample() first argument has to be a field name. Instead given: " + String.valueOf(arg0));
                    }
                    final NameExpression nameExpression = (NameExpression) arg0;
                    final String fieldName = nameExpression.name;
                    final Field field = getField(fieldName, datasetMetadata);
                    fieldNames.add(fieldName);
                    final int numerator = Math.max(0, parseInt(input.get(1)));
                    final int denominator = Math.max(1, Math.max(numerator, input.size() >= 3 ? parseInt(input.get(2)) : 100));
                    final String salt;
                    if(input.size() >= 4) {
                        final String userSalt = Strings.nullToEmpty(getStr(input.get(3)));
                        salt = userSalt.substring(0, Math.min(userSalt.length(), 32));  // limit salt length to 32 char just in case
                    } else {
                        // generate a new salt
                        salt = String.valueOf(System.nanoTime() % Integer.MAX_VALUE);
                    }
                    return new SampleCondition(field, (double) numerator / denominator, salt, negation);
                    // we can also do it through a predicate condition but that requires FTGS instead of a regroup
                }
            });

            functionLookup = builder.build();
        }

        @Override
        protected List<Condition> binaryExpression(final Expression left, final Op op, final Expression right) {
            boolean usingNegation = negation;   // local copy so that it can be modified independently
            switch (op) {
                case NOT_IN:
                    usingNegation = !usingNegation;
                    // fall through to IN
                case IN:
                    {
                        final NameExpression name = (NameExpression) left;
                        final TupleExpression values = (TupleExpression) right;
                        if (datasetMetadata.hasStringField(name.name)) {
                            // TODO how do we handle tokenized fields here?
                            fieldNames.add(name.name);
                            final String[] strings = new String[values.expressions.size()];
                            int index = 0;
                            for (Expression expression : values.expressions) {
                                strings[index++] = getStr(expression);
                            }
                            Arrays.sort(strings);   // looks like terms being sorted is a pre-requisite of stringOrRegroup()
                            return Lists.<Condition>newArrayList(new StringInCondition(Field.stringField(name.name), usingNegation, false, strings));
                        } else if (datasetMetadata.hasIntField(name.name)) {
                            fieldNames.add(name.name);
                            final long[] ints = new long[values.expressions.size()];
                            int index = 0;
                            for (Expression expression : values.expressions) {
                                if(!(expression instanceof NumberExpression)) {
                                    throw new IqlKnownException.FieldTypeMismatchException("A non integer value specified for an integer field: " + name.name);
                                }
                                ints[index++] = parseLong(expression);
                            }
                            Arrays.sort(ints); // looks like terms being sorted is a pre-requisite of intOrRegroup()
                            return Lists.<Condition>newArrayList(new IntInCondition(Field.intField(name.name), usingNegation, ints));
                        } else {
                            throw new IqlKnownException.UnknownFieldException("Unknown field: " + name.name);
                        }
                    }
                case NOT_EQ:
                    usingNegation = !usingNegation;
                    // fall through to EQ
                case EQ:
                    if(left instanceof NameExpression) {
                        final NameExpression name = (NameExpression) left;
                        if(datasetMetadata.hasField(name.name)) {
                            fieldNames.add(name.name);
                            return handleFieldComparison(name, right, usingNegation);
                        }
                    } else if(right instanceof NumberExpression) {
                        return handleMetricComparison(left, right, usingNegation);
                    } else if (!(left instanceof StringExpression || right instanceof StringExpression)) {
                        // assume we have a comparison of 2 metrics. filter for the result of that = 1
                        return handleMetricComparison(new BinaryExpression(left, Op.EQ, right),
                                new NumberExpression("1"), usingNegation);
                    } else {
                        throw new IqlKnownException.ParseErrorException("Can't compare the provided operands: " + left + "; " + right);
                    }
                case REGEX_NOT_EQ:
                    usingNegation = !usingNegation;
                    // fall through to REGEX_EQ
                case REGEX_EQ:
                    if(!(left instanceof NameExpression)) {
                        throw new IqlKnownException.ParseErrorException("Regexp compare only works on field names. Instead given: " + String.valueOf(left));
                    }
                    final NameExpression nameExpression = (NameExpression) left;
                    final String fieldName = nameExpression.name;
                    if (!datasetMetadata.hasStringField(fieldName)) {
                        if(datasetMetadata.hasIntField(fieldName)) {
                            throw new IqlKnownException.ParseErrorException("Regex filter currently only works on String fields. " +
                                    "Int field given: " + fieldName);
                        }
                        throw new IqlKnownException.UnknownFieldException("Unknown field: " + fieldName);
                    }
                    fieldNames.add(fieldName);
                    String regexp = getStr(right);
                    // validate the provided regex
                    try {
                        new RegExp(regexp).toAutomaton();
                    } catch (Exception e) {
                        Throwables.propagateIfInstanceOf(e, RegexTooComplexException.class);

                        throw new IqlKnownException.ParseErrorException("The provided regex filter '" + regexp + "' failed to parse. " +
                                "\nError was: " + e.getMessage() +
                                "\nThe supported regex syntax can be seen here: http://www.brics.dk/automaton/doc/index.html?dk/brics/automaton/RegExp.html", e);
                    }
                    return Collections.<Condition>singletonList(new RegexCondition(Field.stringField(fieldName), regexp,
                        usingNegation));
                case AND:
                    final List<Condition> ret = Lists.newArrayList();
                    ret.addAll(left.match(this));
                    ret.addAll(right.match(this));
                    return ret;
                case LESS:
                case LESS_EQ:
                case GREATER:
                case GREATER_EQ:
                    if ((left instanceof  StringExpression || right instanceof StringExpression)) {
                        throw new IqlKnownException.ParseErrorException(op.toString() + " operation can't be applied to a string");
                    }
                    if(left instanceof NameExpression && right instanceof NumberExpression) {
                        final Stat stat = left.match(statMatcher);
                        long value = parseLong(right);    // constant we are comparing against
                        if (op == Op.LESS) {
                            value -= 1;
                        } else if (op == Op.GREATER) {
                            value += 1;
                        }
                        final long min;
                        final long max;
                        if (op == Op.LESS || op == Op.LESS_EQ) {
                            min = Long.MIN_VALUE;
                            max = value;
                        } else { // GREATER / GREATER_EQ
                            min = value;
                            max = Long.MAX_VALUE;
                        }
                        return Collections.<Condition>singletonList(new MetricCondition(stat, min, max, negation));
                    } else {
                        // assume we have a comparison of 2 metrics. filter for the result of that = 1
                        return handleMetricComparison(new BinaryExpression(left, op, right),
                                new NumberExpression("1"), negation);
                    }
                case PLUS:
                case MINUS:
                case MUL:
                case DIV:
                case AGG_DIV:
                case MOD:
                    throw new IqlKnownException.ParseErrorException(op.toString() + " operation is not usable as a filter");
                default:
                    throw new IllegalStateException();
            }
        }

        private List<Condition> handleMetricComparison(Expression left, Expression right, boolean usingNegation) {
            final Stat stat;
            try {
                stat = left.match(statMatcher);
            } catch (Exception e) {
                throw new IqlKnownException.ParseErrorException("Left side of comparison is not a known field or metric: " + left.toString());
            }
            if (!(right instanceof NumberExpression)) {
                throw new IqlKnownException.ParseErrorException("Metric comparison values have to be numbers");
            }
            final long value = parseLong(right);    // constant we are comparing against

            return Collections.<Condition>singletonList(new MetricCondition(stat, value, value, usingNegation));
        }

        private List<Condition> handleFieldComparison(NameExpression name, Expression right, boolean usingNegation) {
            if (datasetMetadata.hasStringField(name.name)) {
                final String value = getStr(right);
                final String[] strings = new String[] { value };
                return Lists.<Condition>newArrayList(new StringInCondition(Field.stringField(name.name), usingNegation, true, strings));
            } else if (datasetMetadata.hasIntField(name.name)) {
                final long[] ints = new long[1];
                if(!(right instanceof NumberExpression)) {
                    throw new IqlKnownException.FieldTypeMismatchException(name.name + " is an integer field and has to be compared to an integer. Instead was given: " + right.toString());
                }
                ints[0] = parseLong(right);
                return Lists.<Condition>newArrayList(new IntInCondition(Field.intField(name.name), usingNegation, ints));
            } else {
                throw new IqlKnownException.UnknownFieldException("Unknown field: " + name.name);
            }
        }

        @Override
        protected List<Condition> unaryExpression(Op op, Expression operand) {
            if(op.equals(Op.NEG)) {
                negation = !negation;
                List<Condition> result = operand.match(this);
                negation = !negation;
                return result;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        protected List<Condition> functionExpression(final String name, final List<Expression> args) {
            final Function<List<Expression>, Condition> function = functionLookup.get(name);
            if (function == null) {
                throw new IllegalArgumentException();
            }
            return Collections.singletonList(function.apply(args));
        }

        @Override
        protected List<Condition> otherwise() {
            throw new UnsupportedOperationException("Syntax error in a Where condition");
        }
    }

    private static com.indeed.flamdex.query.Query parseLuceneQuery(String queryString, DatasetMetadata datasetMetadata) {
        final Analyzer analyzer = new KeywordAnalyzer();
        final QueryParser queryParser = new QueryParser("foo", analyzer);
        queryParser.setDefaultOperator(QueryParser.Operator.AND);
        // only auto-lowercase for non-Flamdex datasets
        queryParser.setLowercaseExpandedTerms(!datasetMetadata.isImhotepDataset());
        final Query query;
        try {
            query = queryParser.parse(queryString);
        } catch (ParseException e) {
            throw new IqlKnownException.ParseErrorException(e);
        }
        return LuceneQueryTranslator.rewrite(query, datasetMetadata.getIntImhotepFieldSet());
    }

    private static final class GroupByMatcher extends Expression.Matcher<Grouping> {
        private static final int MAX_RECOMMENDED_BUCKETS = 1000;

        private final Map<String, Function<List<Expression>, Grouping>> functionLookup;

        private final DatasetMetadata datasetMetadata;
        private final DateTime start;
        private final DateTime end;
        private final Limits limits;
        private final int rowLimit;

        private final StatMatcher statMatcher;
        private final Set<String> fieldNames;


        private GroupByMatcher(final DatasetMetadata datasetMetadata, final DateTime start, final DateTime end,
                               final int rowLimit, final Limits limits, final Set<String> fieldNames) {
            statMatcher = new StatMatcher(datasetMetadata, fieldNames);
            this.rowLimit = rowLimit;
            this.datasetMetadata = datasetMetadata;
            this.start = start;
            this.end = end;
            this.limits = limits;
            this.fieldNames = fieldNames;
            final ImmutableMap.Builder<String, Function<List<Expression>, Grouping>> builder = ImmutableMap.builder();
            builder.put("topterms", new Function<List<Expression>, Grouping>() {
                public Grouping apply(final List<Expression> input) {
                    if (input.size() < 2 || input.size() > 4) {
                        throw new IqlKnownException.ParseErrorException("topterms() takes 2 to 4 arguments. " + input.size() + " given");
                    }
                    final String fieldName = getName(input.get(0));
                    final int topK = parseInt(input.get(1));
                    final Stat stat;
                    if (input.size() >= 3) {
                        stat = input.get(2).match(statMatcher);
                    } else {
                        stat = counts();
                    }
                    final boolean bottom;
                    if(input.size() >= 4) {
                        String ascDesc = getStr(input.get(3));
                        bottom = ascDesc.equals("bottom");
                    } else {
                        bottom = false;
                    }

                    final Field field = getField(fieldName, datasetMetadata);
                    fieldNames.add(fieldName);
                    return new FieldGrouping(field, topK, stat, bottom, limits);
                }
            });

            builder.put("diff", new Function<List<Expression>, Grouping>() {
                public Grouping apply(final List<Expression> input) {
                    if (input.size() != 4) {
                        throw new IqlKnownException.ParseErrorException("diff() takes 4 args: fieldName(string), metricFilter1(StatExpression), metricFilter2(StatExpression), topK(int)");
                    }
                    final String fieldName = getName(input.get(0));
                    Stat statFilter1 = input.get(1).match(statMatcher);
                    Stat statFilter2 = input.get(2).match(statMatcher);
                    final int topK = parseInt(input.get(3));

                    final Field field = getField(fieldName, datasetMetadata);
                    fieldNames.add(fieldName);
                    return new DiffGrouping(field, statFilter1, statFilter2, topK);
                }
            });

            Function<List<Expression>, Grouping> bucketHandler =
                new Function<List<Expression>, Grouping>() {
                    public Grouping apply(final List<Expression> input) {
                        if (input.size() == 4 || input.size() == 5) {
                            final long min = parseLong(input.get(1));
                            final long max = parseLong(input.get(2));
                            final long interval = parseTimeBucketInterval(getStr(input.get(3)), false, 0, 0);
                            boolean noGutters = false;
                            if(input.size() == 5) {
                                final String noGuttersStr = getStr(input.get(4));
                                noGutters = "true".equalsIgnoreCase(noGuttersStr) || "1".equals(noGuttersStr);
                            }
                            return new StatRangeGrouping(input.get(0).match(statMatcher), min, max, interval, noGutters,
                                    new LongStringifier(), false, limits);
                        } else if (input.size() == 8) {
                            // DEPRECATED: queries using buckets() with 8 args should be rewritten as 2 buckets() groupings with 4 args each
                            final Stat xStat = input.get(0).match(statMatcher);
                            final long xMin = parseLong(input.get(1));
                            final long xMax = parseLong(input.get(2));
                            final long xInterval = parseTimeBucketInterval(getStr(input.get(3)), false, 0, 0);
                            final Stat yStat = input.get(4).match(statMatcher);
                            final long yMin = parseLong(input.get(5));
                            final long yMax = parseLong(input.get(6));
                            final long yInterval = parseTimeBucketInterval(getStr(input.get(7)), false, 0, 0);
                            return new StatRangeGrouping2D(xStat, xMin, xMax, xInterval, yStat, yMin, yMax, yInterval, limits);
                        } else {
                            throw new IqlKnownException.ParseErrorException("buckets() takes 4 or 5 arguments: stat, min(long), max(long), bucket_size(long), [noGutters(boolean)]");
                        }
                    }
                };
            builder.put("bucket", bucketHandler);
            builder.put("buckets", bucketHandler);

            Function<List<Expression>, Grouping> timeHandler = new Function<List<Expression>, Grouping>() {
                public Grouping apply(final List<Expression> input) {
                    if (input.size() > 3) {
                        throw new IqlKnownException.ParseErrorException("time function takes up to 3 args");
                    }
                    final String bucket = input.size() > 0 ? getStr(input.get(0)) : null;
                    final String format = input.size() > 1 ? getStr(input.get(1)) : null;
                    final Expression timeField = input.size() > 2 ? input.get(2) : null;

                    return timeBuckets(bucket, format, timeField);
                }
            };
            builder.put("timebuckets", timeHandler);
            builder.put("time", timeHandler);
            functionLookup = builder.build();
        }

        private Grouping timeBuckets(String bucket, String format, Expression timeField) {
            final int min = (int) (start.getMillis()/1000);
            final int max = (int) (end.getMillis()/1000);
            final long interval = parseTimeBucketInterval(bucket, true, min, max);
            final DateTimeFormatter dateTimeFormatter;
            if (format != null) {
                dateTimeFormatter = DateTimeFormat.forPattern(format);
            } else {
                dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
            }
            final Stringifier<Long> stringifier = new Stringifier<Long>() {
                public String toString(final Long integer) {
                    return new DateTime(integer*1000).toString(dateTimeFormatter);
                }

                public Long fromString(final String str) {
                    return (new DateTime(str).getMillis()/1000);
                }
            };
            final Stat stat;
            if (timeField != null) {
                stat = timeField.match(statMatcher);
            } else {
                // TODO: time field inference?
                stat = intField(datasetMetadata.getTimeFieldName());
            }
            return new StatRangeGrouping(stat, min, max, interval, false, stringifier, true, limits);
        }



        private long parseTimeBucketInterval(String bucketSizeStr, boolean isTime, int min, int max) {
            if(Strings.isNullOrEmpty(bucketSizeStr)) {
                bucketSizeStr = inferTimeBucketSize();
            }

            long bucketSize;

            if(StringUtils.isNumeric(bucketSizeStr)) {
                // given a pure number
                bucketSize = Long.parseLong(bucketSizeStr);
                if(isTime) {
                    // no suffix specified for a time bucket size.
                    // assume hours instead of seconds to avoid overflows due to unintended second buckets
                    bucketSize *= SECONDS_IN_HOUR;
                }
            } else if(bucketSizeStr.charAt(bucketSizeStr.length()-1) == 'b' && min > 0 && max > 0) {
                // given the number of buckets instead of the bucket size. so compute the bucket size ourselves
                final String bucketCountStr = bucketSizeStr.substring(0, bucketSizeStr.length() - 1);
                if (!NumberUtils.isDigits(bucketCountStr)) {
                    throw new IqlKnownException.ParseErrorException("Bucket size argument couldn't be parsed: " + bucketSizeStr);
                }
                final int bucketCount = Integer.parseInt(bucketCountStr);
                if(bucketCount < 1) {
                    throw new IqlKnownException.ParseErrorException("Number of time buckets has to be at least 1");
                }
                return (long)Math.ceil((max-min) / (double)bucketCount); // bucket size rounded up
            } else {
                Period period = PeriodParser.parseString(bucketSizeStr);
                if(period == null) {
                    throw new IqlKnownException.ParseErrorException("Bucket size argument couldn't be parsed: " + bucketSizeStr);
                }
                if(period.getMonths() > 0 || period.getYears() > 0) {
                    throw new IqlKnownException.ParseErrorException("Months and years are not supported as bucket sizes because they vary in length. " +
                            "Please convert to a fixed period (e.g days, weeks) or request an absolute number of buckets (e.g. 5b)");
                }
                bucketSize =  period.toStandardSeconds().getSeconds();
            }

            // validate time period bucketing is compatible with the given time range
            if(isTime) {
                int xMin = (int)(start.getMillis() / 1000);
                int xMax = (int)(end.getMillis() / 1000);
                long timePeriod = xMax - xMin;

                if (timePeriod % bucketSize != 0) {
                    StringBuilder exceptionBuilder = new StringBuilder("You requested a time period (");
                    appendTimePeriod(timePeriod, exceptionBuilder);
                    exceptionBuilder.append(") not evenly divisible by the bucket size (");
                    appendTimePeriod(bucketSize, exceptionBuilder);
                    exceptionBuilder.append("). To correct, increase the time range by ");
                    appendTimePeriod(bucketSize - timePeriod%bucketSize, exceptionBuilder);
                    exceptionBuilder.append(" or reduce the time range by ");
                    appendTimePeriod(timePeriod%bucketSize, exceptionBuilder);
                    throw new IqlKnownException.ParseErrorException(exceptionBuilder.toString());
                }
            }

            return bucketSize;
        }

        private static int appendTimePeriod(long timePeriod, StringBuilder builder) {
            final int timePeriodUnits;
            if (timePeriod % SECONDS_IN_WEEK == 0) {
                // duration is in days
                builder.append(timePeriod / SECONDS_IN_WEEK);
                builder.append(" weeks");
                timePeriodUnits = SECONDS_IN_WEEK;
            } else if (timePeriod % SECONDS_IN_DAY == 0) {
                // duration is in days
                builder.append(timePeriod / SECONDS_IN_DAY);
                builder.append(" days");
                timePeriodUnits = SECONDS_IN_DAY;
            } else if (timePeriod % SECONDS_IN_HOUR == 0) {
                // duration is in hours
                builder.append(timePeriod / SECONDS_IN_HOUR);
                builder.append(" hours");
                timePeriodUnits = SECONDS_IN_HOUR;
            } else if (timePeriod % SECONDS_IN_MINUTE == 0) {
                // duration is in minutes
                builder.append(timePeriod / SECONDS_IN_MINUTE);
                builder.append(" minutes");
                timePeriodUnits = SECONDS_IN_MINUTE;
            } else {
                // duration is seconds
                builder.append(timePeriod);
                builder.append(" seconds");
                timePeriodUnits = 1;
            }
            return timePeriodUnits;
        }

        private static final int SECONDS_IN_MINUTE = 60;
        private static final int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
        private static final int SECONDS_IN_DAY = SECONDS_IN_HOUR * 24;
        private static final int SECONDS_IN_WEEK = SECONDS_IN_DAY * 7;


        protected Grouping functionExpression(final String name, final List<Expression> args) {
            final Function<List<Expression>, Grouping> function = functionLookup.get(name);
            if (function == null) {
                throw new IqlKnownException.ParseErrorException("Unknown function in group by: " + name);
            }
            return function.apply(args);
        }

        protected Grouping nameExpression(final String name) {
            if("time".equals(name)) {   // time buckets special case
                return timeBuckets(null, null, null);
            } // else // normal simple field grouping

            final Field field = getField(name, datasetMetadata);
            fieldNames.add(name);
            return new FieldGrouping(field, true, rowLimit, limits);
        }

        @Override
        protected Grouping bracketsExpression(final String field, final String content) {
            return topTerms(field, content);
        }

        @Override
        protected Grouping unaryExpression(Op op, Expression operand) {
            switch (op) {
                // Deprecated
//                case EXPLODE:
//                {
//                    final String fieldName = getStr(operand);
//                    final Field field = getField(fieldName, datasetMetadata);
//                    return new FieldGrouping(field, false, rowLimit);
//                }
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected Grouping binaryExpression(final Expression left, final Op op, final Expression right) {
            switch (op) {
                case IN:
                {
                    final NameExpression name = (NameExpression) left;
                    final TupleExpression values = (TupleExpression) right;
                    List<String> terms = Lists.newArrayListWithCapacity(values.expressions.size());
                    for (Expression expression : values.expressions) {
                        terms.add(getStr(expression));
                    }
                    final Field field = getField(name.name, datasetMetadata);
                    fieldNames.add(name.name);
                    return new FieldGrouping(field, true, terms, limits);
                }
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private String inferTimeBucketSize() {
            Period period = new Period(start, end,
                    PeriodType.forFields(new DurationFieldType[]{DurationFieldType.weeks(), DurationFieldType.days(),
                            DurationFieldType.hours(), DurationFieldType.minutes(), DurationFieldType.seconds()})
            );
            // try various sizes from smallest to largest until we find one that gives us number of buckets no more than we want
            for(int i = 0; i <= 4; i++) {
                int buckets;
                String value;
                switch (i) {
                    case 4: {
                        buckets = period.toStandardWeeks().getWeeks();
                        value = "1w";
                        break;
                    }
                    case 3: {
                        buckets = period.toStandardDays().getDays();
                        value = "1d";
                        break;
                    }
                    case 2: {
                        buckets = period.toStandardHours().getHours();
                        value = "1h";
                        break;
                    }
                    case 1: {
                        buckets = period.toStandardMinutes().getMinutes();
                        value = "1m";
                        break;
                    }
                    case 0: {
                        buckets = period.toStandardSeconds().getSeconds();
                        value = "1s";
                        break;
                    }
                    default: {
                        throw new RuntimeException("Shouldn't happen");
                    }
                }
                if(buckets < MAX_RECOMMENDED_BUCKETS) {
                    return value;
                }
            }
            // we should never get here but just in case
            return "1w";
        }


        private final String syntaxExamples =
                "Syntax examples:" +
                "\nTop terms: country[top 5 by sjc]" +
                "\nBucketing: buckets(oji, 0, 10, 1)";

        private Grouping topTerms(String fieldName, String arg) {
            if(arg == null || arg.trim().isEmpty()) {
                // treat as a request to get all terms but not explode
                final Field field = getField(fieldName, datasetMetadata);
                fieldNames.add(fieldName);
                return new FieldGrouping(field, true, rowLimit, limits);
            }

            Pattern topTermsPattern = Pattern.compile("\\s*(?:(top|bottom)\\s+)?(\\d+)\\s*(?:\\s*(?:by|,)\\s*(.+))?\\s*");
            Matcher matcher = topTermsPattern.matcher(arg);
            if(!matcher.matches()) {
                throw new IqlKnownException.ParseErrorException("'group by' part treated as top terms couldn't be parsed: " +
                        fieldName + "[" + arg + "].\n" + syntaxExamples);
            }

            final int topK = Integer.parseInt(matcher.group(2));
            final Stat stat;
            String statStr = matcher.group(3);
            if (!Strings.isNullOrEmpty(statStr)) {
                try {
                    Expression statExpression = ExpressionParser.parseExpression(statStr);
                    stat = statExpression.match(statMatcher);
                } catch (Exception e) {
                    throw new IqlKnownException.ParseErrorException("Couldn't parse the stat expression for top terms: " + statStr +
                            "\n" + syntaxExamples, e);
                }
            } else {
                stat = counts();
            }
            final boolean bottom = "bottom".equals(matcher.group(1));

            final Field field = getField(fieldName, datasetMetadata);
            fieldNames.add(fieldName);
            return new FieldGrouping(field, topK, stat, bottom, limits);
        }
    }

    private static int parseInt(Expression expression) {
        return (int) parseLong(expression);
    }

    private static long parseLong(Expression expression) {
        return expression.match(GET_LONG);
    }

    private static final Expression.Matcher<Long> GET_LONG = new Expression.Matcher<Long>() {
        protected Long numberExpression(final String value) {
            return Long.parseLong(value);
        }

        @Override
        protected Long unaryExpression(Op op, Expression operand) {
            if(operand instanceof NumberExpression) {
                final String stringValue = "-" + ((NumberExpression)operand).number;
                return Long.parseLong(stringValue);
            } else {
                throw new UnsupportedOperationException("Expected a number to negate, got " + operand.toString());
            }
        }
    };

    private static final Expression.Matcher<String> GET_STR = new Expression.Matcher<String>() {
        protected String numberExpression(final String value) {
            return value;
        }

        protected String stringExpression(final String value) {
            return value;
        }

        protected String nameExpression(final String value) {
            return value;
        }
    };

    private static String getStr(Expression expression) {
        return expression.match(GET_STR);
    }

    private static String getName(Expression expression) {
        return ((NameExpression)expression).name;
    }
}
