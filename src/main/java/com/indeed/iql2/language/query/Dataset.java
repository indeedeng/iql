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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocFilters;
import com.indeed.iql2.language.JQLBaseListener;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.ParserCommon;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.indeed.iql2.language.Identifiers.parseIdentifier;

public class Dataset extends AbstractPositional {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public final Positioned<String> dataset;
    public final Positioned<DateTime> startInclusive;
    public final Positioned<DateTime> endExclusive;
    public final Optional<Positioned<String>> alias;
    public final ImmutableMap<Positioned<String>, Positioned<String>> fieldAliases;

    public Dataset(Positioned<String> dataset, Positioned<DateTime> startInclusive, Positioned<DateTime> endExclusive,
                   Optional<Positioned<String>> alias, Map<Positioned<String>, Positioned<String>> fieldAliases) {
        this.dataset = dataset;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
        this.alias = alias;
        this.fieldAliases = ImmutableMap.copyOf(fieldAliases);
    }

    public Positioned<String> getDisplayName() {
        return alias.or(dataset);
    }

    public static List<Pair<Dataset, Optional<DocFilter>>> parseDatasets(final Query.Context context) {
        final List<Pair<Dataset, Optional<DocFilter>>> result = new ArrayList<>();
        final Pair<Dataset, Optional<DocFilter>> ds1 = parseDataset(context.fromContext.dataset(), context);
        result.add(ds1);
        for (final JQLParser.DatasetOptTimeContext dataset : context.fromContext.datasetOptTime()) {
            result.add(parsePartialDataset(ds1.getFirst().startInclusive.unwrap(), ds1.getFirst().endExclusive.unwrap(), dataset, context));
        }
        return result;
    }

    public static Pair<Dataset, Optional<DocFilter>> parseDataset(
            final JQLParser.DatasetContext datasetContext,
            Query.Context context) {
        ScopedFieldResolver fieldResolver = context.fieldResolver;
        final Positioned<String> dataset = fieldResolver.resolveImhotepDataset(datasetContext.index);
        final Positioned<DateTime> start = parseDateTime(datasetContext.start, datasetContext.useLegacy, context.clock);
        final Positioned<DateTime> end = parseDateTime(datasetContext.end, datasetContext.useLegacy, context.clock);
        final Optional<Positioned<String>> name;
        if (datasetContext.name != null) {
            name = Optional.of(parseIdentifier(datasetContext.name));
        } else {
            name = Optional.absent();
        }

        // Overwrite variables to avoid accidentally using the wrong one.
        final String resolvedDataset = fieldResolver.resolveDataset(name.or(dataset).unwrap());
        fieldResolver = fieldResolver.forScope(Collections.singleton(resolvedDataset));
        context = context.withFieldResolver(fieldResolver);

        final Map<Positioned<String>, Positioned<String>> fieldAliases = parseFieldAliases(datasetContext.aliases(), fieldResolver);
        final Optional<DocFilter> initializerFilter;
        if (datasetContext.whereContents() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : datasetContext.whereContents().docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx, context));
            }
            initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilters.and(filters)));
        } else {
            initializerFilter = Optional.absent();
        }
        checkRange(start.unwrap(), end.unwrap());
        final Dataset dataset1 = new Dataset(dataset, start, end, name, fieldAliases);
        dataset1.copyPosition(datasetContext);
        return Pair.of(dataset1, initializerFilter);
    }

    public static Pair<Dataset, Optional<DocFilter>> parsePartialDataset(
            final DateTime defaultStart,
            final DateTime defaultEnd,
            final JQLParser.DatasetOptTimeContext datasetOptTimeContext,
            final Query.Context context) {
        final Object[] ref = new Object[1];
        final ScopedFieldResolver fieldResolver = context.fieldResolver;

        datasetOptTimeContext.enterRule(new JQLBaseListener() {
            private void accept(Pair<Dataset, Optional<DocFilter>> value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterFullDataset(JQLParser.FullDatasetContext ctx) {
                accept(parseDataset(ctx.dataset(), context));
            }

            public void enterPartialDataset(JQLParser.PartialDatasetContext ctx) {
                final Positioned<String> dataset = fieldResolver.resolveImhotepDataset(ctx.index);
                final Optional<Positioned<String>> name;
                if (ctx.name != null) {
                    name = Optional.of(parseIdentifier(ctx.name));
                } else {
                    name = Optional.absent();
                }

                final String resolvedDataset = fieldResolver.resolveDataset(name.or(dataset).unwrap());
                final ScopedFieldResolver datasetFieldResolver = fieldResolver.forScope(Collections.singleton(resolvedDataset));
                final Query.Context datasetContext = context.withFieldResolver(datasetFieldResolver);

                final Map<Positioned<String>, Positioned<String>> fieldAliases = parseFieldAliases(ctx.aliases(), datasetFieldResolver);

                final Optional<DocFilter> initializerFilter;
                if (ctx.whereContents() != null) {
                    final List<DocFilter> filters = new ArrayList<>();
                    for (final JQLParser.DocFilterContext filterCtx : ctx.whereContents().docFilter()) {
                        filters.add(DocFilters.parseDocFilter(filterCtx, datasetContext));
                    }
                    initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilters.and(filters)));
                } else {
                    initializerFilter = Optional.absent();
                }

                checkRange(defaultStart, defaultEnd); // this should not fail as we already checked this range before, but just in case.
                final Dataset dataset1 = new Dataset(dataset, Positioned.unpositioned(defaultStart), Positioned.unpositioned(defaultEnd), name, fieldAliases);
                dataset1.copyPosition(ctx);
                accept(Pair.of(dataset1, initializerFilter));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled partialDataset: " + datasetOptTimeContext.getText());
        }

        return (Pair<Dataset, Optional<DocFilter>>) ref[0];
    }

    private static Map<Positioned<String>, Positioned<String>> parseFieldAliases(JQLParser.AliasesContext aliases, final ScopedFieldResolver fieldResolver) {
        if (aliases == null) {
            return Collections.emptyMap();
        }
        final Map<Positioned<String>, Positioned<String>> result = new HashMap<>();
        for (int i = 0; i < aliases.virtual.size(); i++) {
            final String actualField = fieldResolver.resolve(aliases.actual.get(i)).getOnlyField();
            final Positioned<String> actual = Positioned.from(actualField, aliases.actual.get(i));
            final Positioned<String> virtual = parseIdentifier(aliases.virtual.get(i));
            result.put(virtual, actual);
        }
        return result;
    }

    public static Positioned<DateTime> parseDateTime(JQLParser.DateTimeContext dateTimeContext, boolean useLegacy, WallClock clock) {
        final String textValue = dateTimeContext.getText();
        final DateTime wordDate = parseWordDate(textValue, useLegacy, clock);
        if (wordDate != null) {
            return Positioned.from(wordDate, dateTimeContext);
        } else {
            if (dateTimeContext.DATETIME_TOKEN() != null) {
                return Positioned.from(createDateTime(dateTimeContext.DATETIME_TOKEN().getText().replaceAll(" ", "T")), dateTimeContext);
            } else if (dateTimeContext.DATE_TOKEN() != null) {
                return Positioned.from(createDateTime(dateTimeContext.DATE_TOKEN().getText()), dateTimeContext);
            } else if (dateTimeContext.STRING_LITERAL() != null) {
                final String unquoted = ParserCommon.unquote(dateTimeContext.STRING_LITERAL().getText());
                try {
                    // Don't use createDataTime method here since error will be caught and processed.
                    return Positioned.from(new DateTime(unquoted.replaceAll(" ", "T")), dateTimeContext);
                } catch (IllegalArgumentException e) {
                    final JQLParser jqlParser = Queries.parserForString(unquoted);
                    final JQLParser.TimePeriodContext timePeriod = jqlParser.timePeriod();
                    if (jqlParser.getNumberOfSyntaxErrors() > 0) {
                        final DateTime dt = parseWordDate(unquoted, useLegacy, clock);
                        if (dt != null) {
                            return Positioned.from(dt, dateTimeContext);
                        }
                        throw new IqlKnownException.ParseErrorException("Failed to parse string as either DateTime or time period: " + unquoted);
                    }
                    return Positioned.from(TimePeriods.timePeriodDateTime(timePeriod, clock, useLegacy), dateTimeContext);
                }
            } else if (dateTimeContext.timePeriod() != null) {
                return Positioned.from(TimePeriods.timePeriodDateTime(dateTimeContext.timePeriod(), clock, useLegacy), dateTimeContext);
            } else if (dateTimeContext.NAT() != null) {
                return Positioned.from(parseUnixTimestamp(dateTimeContext.NAT().getText()), dateTimeContext);
            }
        }
        throw new IqlKnownException.ParseErrorException("Unhandled dateTime: " + dateTimeContext.getText());
    }

    private static DateTime parseUnixTimestamp(String value) {
        long timestamp = Long.parseLong(value);
        if(timestamp < Integer.MAX_VALUE) {
            timestamp *= 1000;  // seconds to milliseconds
        }
        return createDateTime(timestamp);
    }

    private static DateTime parseWordDate(String textValue, boolean useLegacy, WallClock clock) {
        final String lowerCasedValue = textValue.toLowerCase();
        if ("yesterday".startsWith(lowerCasedValue)) {
            return createDateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().minusDays(1);
        } else if ("ago".equals(lowerCasedValue) || ((useLegacy || (textValue.length() >= 3)) && "today".startsWith(lowerCasedValue))) {
            return createDateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        } else if (textValue.length() >= 3 && "tomorrow".startsWith(lowerCasedValue)) {
            return createDateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().plusDays(1);
        }
        return null;
    }

    private static DateTime createDateTime(final long time) {
        try {
            return new DateTime(time);
        } catch (final Throwable t) {
            throw new IqlKnownException.ParseErrorException("Error parsing date/time: " + time, t);
        }
    }

    private static DateTime createDateTime(final String time) {
        try {
            return new DateTime(time);
        } catch (final Throwable t) {
            throw new IqlKnownException.ParseErrorException("Error parsing date/time: " + time, t);
        }
    }

    private static void checkRange(final DateTime start, final DateTime end) {
        if(!end.isAfter(start)) {
            throw new IqlKnownException.ParseErrorException("Illegal time range requested: " + start.toString() + " to " + end.toString());
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Dataset dataset1 = (Dataset) o;
        return Objects.equal(dataset, dataset1.dataset) &&
                Objects.equal(startInclusive, dataset1.startInclusive) &&
                Objects.equal(endExclusive, dataset1.endExclusive) &&
                Objects.equal(alias, dataset1.alias) &&
                Objects.equal(fieldAliases, dataset1.fieldAliases);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataset, startInclusive, endExclusive, alias, fieldAliases);
    }

    @Override
    public String toString() {
        return "Dataset{" +
                "dataset='" + dataset + '\'' +
                ", startInclusive=" + startInclusive +
                ", endExclusive=" + endExclusive +
                ", alias=" + alias +
                '}';
    }
}
