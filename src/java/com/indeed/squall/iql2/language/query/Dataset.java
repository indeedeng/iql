package com.indeed.squall.iql2.language.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.Identifiers;
import com.indeed.squall.iql2.language.JQLBaseListener;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.TimePeriods;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.util.core.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

public class Dataset {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public final String dataset;
    public final DateTime startInclusive;
    public final DateTime endExclusive;
    public final Optional<String> alias;
    public final ImmutableMap<String, String> fieldAliases;

    public Dataset(String dataset, DateTime startInclusive, DateTime endExclusive, Optional<String> alias, Map<String, String> fieldAliases) {
        this.dataset = dataset;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
        this.alias = alias;
        this.fieldAliases = ImmutableMap.copyOf(fieldAliases);
    }

    public static List<Pair<Dataset, Optional<DocFilter>>> parseDatasets(JQLParser.FromContentsContext fromContentsContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        final List<Pair<Dataset, Optional<DocFilter>>> result = new ArrayList<>();
        final Pair<Dataset, Optional<DocFilter>> ds1 = parseDataset(fromContentsContext.dataset(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
        result.add(ds1);
        for (final JQLParser.DatasetOptTimeContext dataset : fromContentsContext.datasetOptTime()) {
            result.add(parsePartialDataset(ds1.getFirst().startInclusive, ds1.getFirst().endExclusive, dataset, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
        }
        return result;
    }

    public static Pair<Dataset, Optional<DocFilter>> parseDataset(JQLParser.DatasetContext datasetContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        final String dataset = parseIdentifier(datasetContext.index);
        final DateTime start = parseDateTime(datasetContext.start, clock);
        final DateTime end = parseDateTime(datasetContext.end, clock);
        final Optional<String> name;
        if (datasetContext.name != null) {
            name = Optional.of(parseIdentifier(datasetContext.name));
        } else {
            name = Optional.absent();
        }
        final Map<String, String> fieldAliases = parseFieldAliases(datasetContext.aliases());
        final Optional<DocFilter> initializerFilter;
        if (datasetContext.whereContents() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : datasetContext.whereContents().docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx, datasetToKeywordAnalyzerFields, datasetToIntFields, null, warn, clock));
            }
            initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset)), DocFilters.and(filters)));
        } else {
            initializerFilter = Optional.absent();
        }
        return Pair.of(new Dataset(dataset, start, end, name, fieldAliases), initializerFilter);
    }

    public static Pair<Dataset, Optional<DocFilter>> parsePartialDataset(final DateTime defaultStart, final DateTime defaultEnd, JQLParser.DatasetOptTimeContext datasetOptTimeContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields, final Consumer<String> warn, final WallClock clock) {
        final Object[] ref = new Object[1];

        datasetOptTimeContext.enterRule(new JQLBaseListener() {
            private void accept(Pair<Dataset, Optional<DocFilter>> value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterFullDataset(JQLParser.FullDatasetContext ctx) {
                accept(parseDataset(ctx.dataset(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
            }

            public void enterPartialDataset(JQLParser.PartialDatasetContext ctx) {
                final String dataset = parseIdentifier(ctx.index);
                final Optional<String> name;
                if (ctx.name != null) {
                    name = Optional.of(parseIdentifier(ctx.name));
                } else {
                    name = Optional.absent();
                }

                final Map<String, String> fieldAliases = parseFieldAliases(ctx.aliases());

                final Optional<DocFilter> initializerFilter;
                if (ctx.whereContents() != null) {
                    final List<DocFilter> filters = new ArrayList<>();
                    for (final JQLParser.DocFilterContext filterCtx : ctx.whereContents().docFilter()) {
                        filters.add(DocFilters.parseDocFilter(filterCtx, datasetToKeywordAnalyzerFields, datasetToIntFields, null, warn, clock));
                    }
                    initializerFilter = Optional.<DocFilter>of(new DocFilter.Qualified(Collections.singletonList(name.or(dataset)), DocFilters.and(filters)));
                } else {
                    initializerFilter = Optional.absent();
                }

                accept(Pair.of(new Dataset(dataset, defaultStart, defaultEnd, name, fieldAliases), initializerFilter));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled partialDataset: " + datasetOptTimeContext.getText());
        }

        return (Pair<Dataset, Optional<DocFilter>>) ref[0];
    }

    private static Map<String, String> parseFieldAliases(JQLParser.AliasesContext aliases) {
        if (aliases == null) {
            return Collections.emptyMap();
        }
        final Map<String, String> result = new HashMap<>();
        for (int i = 0; i < aliases.virtual.size(); i++) {
            final String actual = parseIdentifier(aliases.actual.get(i));
            final String virtual = parseIdentifier(aliases.virtual.get(i));
            result.put(virtual, actual);
        }
        return result;
    }

    public static DateTime parseDateTime(JQLParser.DateTimeContext dateTimeContext, WallClock clock) {
        if (dateTimeContext.DATETIME_TOKEN() != null) {
            return new DateTime(dateTimeContext.DATETIME_TOKEN().getText().replaceAll(" ", "T"));
        } else if (dateTimeContext.DATE_TOKEN() != null) {
            return new DateTime(dateTimeContext.DATE_TOKEN().getText());
        } else if (dateTimeContext.STRING_LITERAL() != null) {
            final String unquoted = ParserCommon.unquote(dateTimeContext.STRING_LITERAL().getText());
            try {
                return new DateTime(unquoted.replaceAll(" ", "T"));
            } catch (IllegalArgumentException e) {
                final JQLParser jqlParser = Queries.parserForString(unquoted);
                final JQLParser.TimePeriodContext timePeriod = jqlParser.timePeriod();
                if (jqlParser.getNumberOfSyntaxErrors() > 0) {
                    final DateTime dt = parseWordDate(unquoted, clock);
                    if (dt != null) {
                        return dt;
                    }
                    throw new IllegalArgumentException("Failed to parse string as either DateTime or time period: " + unquoted);
                }
                return TimePeriods.timePeriodDateTime(timePeriod, clock);
            }
        } else if (dateTimeContext.timePeriod() != null) {
            return TimePeriods.timePeriodDateTime(dateTimeContext.timePeriod(), clock);
        } else if (dateTimeContext.NAT() != null) {
            return new DateTime(Long.parseLong(dateTimeContext.NAT().getText()));
        } else {
            final String textValue = dateTimeContext.getText();
            final DateTime dt = parseWordDate(textValue, clock);
            if (dt != null) return dt;
        }
        throw new UnsupportedOperationException("Unhandled dateTime: " + dateTimeContext.getText());
    }

    private static DateTime parseWordDate(String textValue, WallClock clock) {
        if ("yesterday".startsWith(textValue.toLowerCase())) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().minusDays(1);
        } else if (textValue.length() >= 3 && "today".startsWith(textValue.toLowerCase())) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        } else if (textValue.length() >= 3 && "tomorrow".startsWith(textValue.toLowerCase())) {
            return new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay().plusDays(1);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dataset dataset1 = (Dataset) o;

        if (dataset != null ? !dataset.equals(dataset1.dataset) : dataset1.dataset != null) return false;
        if (startInclusive != null ? !startInclusive.equals(dataset1.startInclusive) : dataset1.startInclusive != null)
            return false;
        if (endExclusive != null ? !endExclusive.equals(dataset1.endExclusive) : dataset1.endExclusive != null)
            return false;
        if (alias != null ? !alias.equals(dataset1.alias) : dataset1.alias != null) return false;
        return !(fieldAliases != null ? !fieldAliases.equals(dataset1.fieldAliases) : dataset1.fieldAliases != null);

    }

    @Override
    public int hashCode() {
        int result = dataset != null ? dataset.hashCode() : 0;
        result = 31 * result + (startInclusive != null ? startInclusive.hashCode() : 0);
        result = 31 * result + (endExclusive != null ? endExclusive.hashCode() : 0);
        result = 31 * result + (alias != null ? alias.hashCode() : 0);
        result = 31 * result + (fieldAliases != null ? fieldAliases.hashCode() : 0);
        return result;
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
