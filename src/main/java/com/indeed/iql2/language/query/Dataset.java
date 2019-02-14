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
import com.google.common.collect.Lists;
import com.indeed.imhotep.Shard;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocFilters;
import com.indeed.iql2.language.JQLBaseListener;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.ParserCommon;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    @Nullable
    public final List<Shard> shards;
    @Nullable
    public final List<Interval> missingShardIntervals;

    private Dataset(
            final Positioned<String> dataset,
            final Positioned<DateTime> startInclusive,
            final Positioned<DateTime> endExclusive,
            final Optional<Positioned<String>> alias,
            final Map<Positioned<String>, Positioned<String>> fieldAliases,
            @Nullable final ShardResolver.ShardResolutionResult shardResolutionResult
    ) {
        this.dataset = dataset;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
        this.alias = alias;
        this.fieldAliases = ImmutableMap.copyOf(fieldAliases);
        if (shardResolutionResult != null) {
            this.shards = Lists.newArrayList(shardResolutionResult.shards);
            Collections.sort(this.shards);
        } else {
            this.shards = null;
        }
        this.missingShardIntervals = (shardResolutionResult != null) ? shardResolutionResult.missingShardTimeIntervals : null;
    }

    public String getDisplayName() {
        return alias.or(dataset).unwrap();
    }

    public static Set<String> datasetToScope(List<Dataset> datasets){
        return datasets.stream().map(Dataset::getDisplayName).collect(Collectors.toSet());
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

        final ShardResolver.ShardResolutionResult resolutionResult = getShards(context, dataset.unwrap(), start.unwrap(), end.unwrap(), name.transform(Positioned::unwrap));

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
            initializerFilter = Optional.of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilter.And.create(filters)));
        } else {
            initializerFilter = Optional.absent();
        }
        checkRange(start.unwrap(), end.unwrap());
        final Dataset dataset1 = new Dataset(dataset, start, end, name, fieldAliases, resolutionResult);
        dataset1.copyPosition(datasetContext);
        return Pair.of(dataset1, initializerFilter);
    }

    private static ShardResolver.ShardResolutionResult getShards(final Query.Context context, final String dataset, final DateTime start, final DateTime end, final Optional<String> name) {
        context.timer.push("resolve shards for dataset " + name.or(dataset));
        final ShardResolver.ShardResolutionResult shardResolutionResult = context.shardResolver.resolve(dataset, start, end);
        context.timer.pop();
        return shardResolutionResult;
    }

    public long numDocs() {
        return shards.stream().mapToLong(Shard::getNumDocs).sum();
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

                final ShardResolver.ShardResolutionResult resolutionResult = getShards(context, dataset.unwrap(), defaultStart, defaultEnd, name.transform(Positioned::unwrap));

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
                    initializerFilter = Optional.of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilter.And.create(filters)));
                } else {
                    initializerFilter = Optional.absent();
                }

                checkRange(defaultStart, defaultEnd); // this should not fail as we already checked this range before, but just in case.
                final Dataset dataset1 = new Dataset(dataset, Positioned.unpositioned(defaultStart), Positioned.unpositioned(defaultEnd), name, fieldAliases, resolutionResult);
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

    public static Positioned<DateTime> parseDateTime(final JQLParser.DateTimeContext dateTimeContext, final boolean useLegacy, final WallClock clock) {
        return Positioned.from(innerParseDateTime(dateTimeContext, useLegacy, clock), dateTimeContext);
    }

    private static DateTime innerParseDateTime(
            final JQLParser.DateTimeContext dateTimeContext,
            final boolean useLegacy,
            final WallClock clock) {
        if (dateTimeContext.DATETIME_TOKEN() != null) {
            return createDateTime(dateTimeContext.DATETIME_TOKEN().getText().replaceAll(" ", "T"));
        } else if (dateTimeContext.STRING_LITERAL() != null) {
            final String unquoted = ParserCommon.unquote(dateTimeContext.STRING_LITERAL().getText());

            // unquoted literal must be parseable by dateTimeTerminal or relativeTimeTerminal

            final JQLParser.DateTimeTerminalContext dateTimeTerminal = Queries.tryRunParser(unquoted, JQLParser::dateTimeTerminal);
            if (dateTimeTerminal != null) {
                return innerParseDateTime(dateTimeTerminal.dateTime(), useLegacy, clock);
            }

            final JQLParser.RelativeTimeTerminalContext relativeTimeTerminal = Queries.tryRunParser(unquoted, JQLParser::relativeTimeTerminal);

            if (relativeTimeTerminal != null) {
                if (relativeTimeTerminal.relativeTime() != null) {
                    // It must be parsed by dateTimeTerminal
                    throw new IllegalStateException("Should not be here!");
                }
                if (relativeTimeTerminal.timeInterval() != null) {
                    final List<Pair<Integer, TimeUnit>> interval = TimePeriods.parseTimeInterval(relativeTimeTerminal.timeInterval().getText(), useLegacy);
                    return TimePeriods.substract(clock, interval);
                }
            }

            throw new IqlKnownException.ParseErrorException("Failed to parse string as either DateTime or time period: " + unquoted);
        } else if (dateTimeContext.relativeTime() != null) {
            return relativeTime(dateTimeContext.relativeTime(), clock, useLegacy);
        } else if (dateTimeContext.NAT() != null) {
            final long timestamp = Long.parseLong(dateTimeContext.NAT().getText());
            // 4 digit number is parsed as NAT but not as DATETIME_TOKEN
            // if it looks like a year, then return year
            if ((timestamp > 2010) && (timestamp < 2050)) {
                return new DateTime((int)timestamp, 1, 1, 0, 0 );
            }
            return parseUnixTimestamp(dateTimeContext.NAT().getText());
        }
        throw new IqlKnownException.ParseErrorException("Unhandled dateTime: " + dateTimeContext.getText());
    }

    public static DateTime relativeTime(
            final JQLParser.RelativeTimeContext context,
            final WallClock clock,
            final boolean useLegacy) {

        if (context.TODAYS() != null) {
            return today(clock);
        }

        if (context.TOMORROWS() != null) {
            return today(clock).plusDays(1);
        }

        if ((context.YESTERDAYS() != null) || (context.Y() != null)) {
            return today(clock).minusDays(1);
        }

        if (context.timeIntervalOneWord() != null) {
            final List<Pair<Integer, TimeUnit>> interval = TimePeriods.parseTimeInterval(context.timeIntervalOneWord().getText(), useLegacy);
            return TimePeriods.substract(clock, interval);
        }

        throw new IqlKnownException.ParseErrorException("Failed to parse string as either DateTime or time period: " + context.getText());
    }

    private static DateTime parseUnixTimestamp(String value) {
        long timestamp = Long.parseLong(value);
        if(timestamp < Integer.MAX_VALUE) {
            timestamp *= 1000;  // seconds to milliseconds
        }
        return createDateTime(timestamp);
    }

    private static DateTime today(final WallClock clock) {
        return createDateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
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

    public static long getLatestEnd(final List<Dataset> datasets) {
        return datasets.stream().map(dataset -> dataset.endExclusive.unwrap().getMillis()).max(Long::compareTo).get();
    }

    public static long getEarliestStart(final List<Dataset> datasets) {
        return datasets.stream().map(dataset -> dataset.startInclusive.unwrap().getMillis()).min(Long::compareTo).get();
    }

    public static long getLongestRange(final List<Dataset> datasets) {
        return datasets.stream().map(dataset -> dataset.endExclusive.unwrap().getMillis() - dataset.startInclusive.unwrap().getMillis()).max(Long::compareTo).get();
    }

    // Used to not consider Host assignment in cache keys
    private static class CacheShard {
        public final String shardId;
        public final long version;

        private CacheShard(final String shardId, final long version) {
            this.shardId = shardId;
            this.version = version;
        }

        public static CacheShard from(final Shard shard) {
            return new CacheShard(shard.shardId, shard.version);
        }

        @Override
        public String toString() {
            return "CacheShard{" +
                    "shardId='" + shardId + '\'' +
                    ", version=" + version +
                    '}';
        }
    }

    @Override
    public boolean equals(final Object o) {
        // fieldAliases deliberately left out due to it not affecting semantics, only prettyprint results
        // missingShardIntervals not used because it's only for diagnostics
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Dataset dataset1 = (Dataset) o;
        return Objects.equal(dataset, dataset1.dataset) &&
                Objects.equal(startInclusive, dataset1.startInclusive) &&
                Objects.equal(endExclusive, dataset1.endExclusive) &&
                Objects.equal(alias, dataset1.alias) &&
                Objects.equal(shards, dataset1.shards);
    }

    @Override
    public int hashCode() {
        // fieldAliases deliberately left out due to it not affecting semantics, only prettyprint results
        // missingShardIntervals not used because it's only for diagnostics
        return Objects.hashCode(dataset, startInclusive, endExclusive, alias, shards);
    }

    @Override
    public String toString() {
        // fieldAliases deliberately left out due to it not affecting semantics, only prettyprint results
        // missingShardIntervals not used because it's only for diagnostics
        return "Dataset{" +
                "dataset=" + dataset +
                ", startInclusive=" + startInclusive +
                ", endExclusive=" + endExclusive +
                ", alias=" + alias +
                ", shards=" + (shards != null ? shards.stream().map(CacheShard::from).collect(Collectors.toList()) : "null") +
                '}';
    }
}
