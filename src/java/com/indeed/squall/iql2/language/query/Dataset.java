package com.indeed.squall.iql2.language.query;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.AbstractPositional;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.JQLBaseListener;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.TimePeriods;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.util.core.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

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

    public Dataset addAliasDimensions(Map<String, String> dimensionAliases) {
        final Map<Positioned<String>, Positioned<String>> newFieldAliases = new HashMap<>();
        dimensionAliases.forEach((key, value) -> {
            // no need to add aliases that targets to itself
            if (!key.equalsIgnoreCase(value)) {
                newFieldAliases.put(Positioned.unpositioned(key), Positioned.unpositioned(value));
            }
        });
        newFieldAliases.putAll(fieldAliases);
        return new Dataset(dataset, startInclusive, endExclusive, alias, newFieldAliases);
    }

    public static Map<String, String> resolveAliasToRealField(Map<Positioned<String>, Positioned<String>> fieldAliases) {
        final Map<String, String> aliasToField = fieldAliases.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().unwrap(),
                e -> e.getValue().unwrap()));
        ImmutableMap.Builder<String, String> resolvedAliasToRealFieldBuilder = new ImmutableMap.Builder<>();
        for (String originField : aliasToField.keySet()) {
            String targetField = aliasToField.get(originField);
            final Set<String> seenField = new LinkedHashSet<>();
            seenField.add(targetField);
            while (aliasToField.containsKey(targetField)) {
                final String newTargetField = aliasToField.get(targetField);
                // for the dimension: same -> same
                if (newTargetField.equals(targetField)) {
                    break;
                }
                if (seenField.contains(newTargetField)) {
                    throw new IllegalArgumentException(
                            String.format("field alias has circular reference: %s -> %s", originField,
                                    Joiner.on(" -> ").join(seenField.toArray())));
                }
                seenField.add(newTargetField);
                targetField = newTargetField;
            }
            resolvedAliasToRealFieldBuilder.put(originField, targetField);
        }
        return resolvedAliasToRealFieldBuilder.build();
    }

    public static List<Pair<Dataset, Optional<DocFilter>>> parseDatasets(JQLParser.FromContentsContext fromContentsContext, DatasetsMetadata datasetsMetadata, Consumer<String> warn, WallClock clock) {
        final List<Pair<Dataset, Optional<DocFilter>>> result = new ArrayList<>();
        final Pair<Dataset, Optional<DocFilter>> ds1 = parseDataset(fromContentsContext.dataset(), datasetsMetadata, warn, clock);
        result.add(ds1);
        for (final JQLParser.DatasetOptTimeContext dataset : fromContentsContext.datasetOptTime()) {
            result.add(parsePartialDataset(ds1.getFirst().startInclusive.unwrap(), ds1.getFirst().endExclusive.unwrap(), dataset, datasetsMetadata, warn, clock));
        }
        return result;
    }

    public static Pair<Dataset, Optional<DocFilter>> parseDataset(JQLParser.DatasetContext datasetContext, DatasetsMetadata datasetsMetadata, Consumer<String> warn, WallClock clock) {
        final Positioned<String> dataset = parseIdentifier(datasetContext.index);
        final Positioned<DateTime> start = parseDateTime(datasetContext.start, datasetContext.useLegacy, clock);
        final Positioned<DateTime> end = parseDateTime(datasetContext.end, datasetContext.useLegacy, clock);
        final Optional<Positioned<String>> name;
        if (datasetContext.name != null) {
            name = Optional.of(parseIdentifier(datasetContext.name));
        } else {
            name = Optional.absent();
        }
        final Map<Positioned<String>, Positioned<String>> fieldAliases = parseFieldAliases(datasetContext.aliases());
        final Optional<DocFilter> initializerFilter;
        if (datasetContext.whereContents() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : datasetContext.whereContents().docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx, datasetsMetadata, null, warn, clock));
            }
            initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilters.and(filters)));
        } else {
            initializerFilter = Optional.absent();
        }
        final Dataset dataset1 = new Dataset(dataset, start, end, name, fieldAliases);
        dataset1.copyPosition(datasetContext);
        return Pair.of(dataset1, initializerFilter);
    }

    public static Pair<Dataset, Optional<DocFilter>> parsePartialDataset(final DateTime defaultStart, final DateTime defaultEnd, JQLParser.DatasetOptTimeContext datasetOptTimeContext, final DatasetsMetadata datasetsMetadata, final Consumer<String> warn, final WallClock clock) {
        final Object[] ref = new Object[1];

        datasetOptTimeContext.enterRule(new JQLBaseListener() {
            private void accept(Pair<Dataset, Optional<DocFilter>> value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterFullDataset(JQLParser.FullDatasetContext ctx) {
                accept(parseDataset(ctx.dataset(), datasetsMetadata, warn, clock));
            }

            public void enterPartialDataset(JQLParser.PartialDatasetContext ctx) {
                final Positioned<String> dataset = parseIdentifier(ctx.index);
                final Optional<Positioned<String>> name;
                if (ctx.name != null) {
                    name = Optional.of(parseIdentifier(ctx.name));
                } else {
                    name = Optional.absent();
                }

                final Map<Positioned<String>, Positioned<String>> fieldAliases = parseFieldAliases(ctx.aliases());

                final Optional<DocFilter> initializerFilter;
                if (ctx.whereContents() != null) {
                    final List<DocFilter> filters = new ArrayList<>();
                    for (final JQLParser.DocFilterContext filterCtx : ctx.whereContents().docFilter()) {
                        filters.add(DocFilters.parseDocFilter(filterCtx, datasetsMetadata, null, warn, clock));
                    }
                    initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset).unwrap()), DocFilters.and(filters)));
                } else {
                    initializerFilter = Optional.absent();
                }

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

    private static Map<Positioned<String>, Positioned<String>> parseFieldAliases(JQLParser.AliasesContext aliases) {
        if (aliases == null) {
            return Collections.emptyMap();
        }
        final Map<Positioned<String>, Positioned<String>> result = new HashMap<>();
        for (int i = 0; i < aliases.virtual.size(); i++) {
            final Positioned<String> actual = parseIdentifier(aliases.actual.get(i));
            final Positioned<String> virtual = parseIdentifier(aliases.virtual.get(i));
            result.put(virtual, actual);
        }
        return result;
    }

    public static Positioned<DateTime> parseDateTime(JQLParser.DateTimeContext dateTimeContext, boolean useLegacy, WallClock clock) {
        if (dateTimeContext.DATETIME_TOKEN() != null) {
            return Positioned.from(new DateTime(dateTimeContext.DATETIME_TOKEN().getText().replaceAll(" ", "T")), dateTimeContext);
        } else if (dateTimeContext.DATE_TOKEN() != null) {
            return Positioned.from(new DateTime(dateTimeContext.DATE_TOKEN().getText()), dateTimeContext);
        } else if (dateTimeContext.STRING_LITERAL() != null) {
            final String unquoted = ParserCommon.unquote(dateTimeContext.STRING_LITERAL().getText());
            try {
                return Positioned.from(new DateTime(unquoted.replaceAll(" ", "T")), dateTimeContext);
            } catch (IllegalArgumentException e) {
                final JQLParser jqlParser = Queries.parserForString(unquoted);
                final JQLParser.TimePeriodContext timePeriod = jqlParser.timePeriod();
                if (jqlParser.getNumberOfSyntaxErrors() > 0) {
                    final DateTime dt = parseWordDate(unquoted, useLegacy, clock);
                    if (dt != null) {
                        return Positioned.from(dt, dateTimeContext);
                    }
                    throw new IllegalArgumentException("Failed to parse string as either DateTime or time period: " + unquoted);
                }
                return Positioned.from(TimePeriods.timePeriodDateTime(timePeriod, clock), dateTimeContext);
            }
        } else if (dateTimeContext.timePeriod() != null) {
            return Positioned.from(TimePeriods.timePeriodDateTime(dateTimeContext.timePeriod(), clock), dateTimeContext);
        } else if (dateTimeContext.NAT() != null) {
            return Positioned.from(new DateTime(Long.parseLong(dateTimeContext.NAT().getText())), dateTimeContext);
        } else {
            final String textValue = dateTimeContext.getText();
            final DateTime dt = parseWordDate(textValue, useLegacy, clock);
            if (dt != null) return Positioned.from(dt, dateTimeContext);
        }
        throw new UnsupportedOperationException("Unhandled dateTime: " + dateTimeContext.getText());
    }

    private static DateTime parseWordDate(String textValue, boolean useLegacy, WallClock clock) {
        final String lowerCasedValue = textValue.toLowerCase();
        if ("yesterday".startsWith(lowerCasedValue)) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().minusDays(1);
        } else if ("ago".equals(lowerCasedValue) || ((useLegacy || (textValue.length() >= 3)) && "today".startsWith(lowerCasedValue))) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        } else if (textValue.length() >= 3 && "tomorrow".startsWith(lowerCasedValue)) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().plusDays(1);
        }
        return null;
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
