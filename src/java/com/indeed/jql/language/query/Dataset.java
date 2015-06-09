package com.indeed.jql.language.query;

import com.google.common.base.Optional;
import com.indeed.jql.language.JQLBaseListener;
import com.indeed.jql.language.JQLParser;
import com.indeed.jql.language.ParserCommon;
import com.indeed.jql.language.TimeUnit;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.misc.NotNull;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

public class Dataset {
    public final String dataset;
    public final DateTime startInclusive;
    public final DateTime endExclusive;
    public final Optional<String> alias;

    public Dataset(String dataset, DateTime startInclusive, DateTime endExclusive, Optional<String> alias) {
        this.dataset = dataset;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
        this.alias = alias;
    }

    public static List<Dataset> parseDatasets(JQLParser.FromContentsContext fromContentsContext) {
        final List<Dataset> result = new ArrayList<>();
        final Dataset ds1 = parseDataset(fromContentsContext.dataset());
        result.add(ds1);
        for (final JQLParser.DatasetOptTimeContext dataset : fromContentsContext.datasetOptTime()) {
            result.add(parsePartialDataset(ds1.startInclusive, ds1.endExclusive, dataset));
        }
        return result;
    }

    public static Dataset parseDataset(JQLParser.DatasetContext datasetContext) {
        final String dataset = datasetContext.index.getText();
        final DateTime start = parseDateTime(datasetContext.start);
        final DateTime end = parseDateTime(datasetContext.end);
        final Optional<String> name;
        if (datasetContext.name != null) {
            name = Optional.of(datasetContext.name.getText());
        } else {
            name = Optional.absent();
        }
        return new Dataset(dataset, start, end, name);
    }

    public static Dataset parsePartialDataset(final DateTime defaultStart, final DateTime defaultEnd, JQLParser.DatasetOptTimeContext datasetOptTimeContext) {
        final Dataset[] ref = new Dataset[1];

        datasetOptTimeContext.enterRule(new JQLBaseListener() {
            private void accept(Dataset value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterFullDataset(@NotNull JQLParser.FullDatasetContext ctx) {
                accept(parseDataset(ctx.dataset()));
            }

            public void enterPartialDataset(@NotNull JQLParser.PartialDatasetContext ctx) {
                final String dataset = ctx.index.getText();
                final Optional<String> name;
                if (ctx.name != null) {
                    name = Optional.of(ctx.name.getText());
                } else {
                    name = Optional.absent();
                }
                accept(new Dataset(dataset, defaultStart, defaultEnd, name));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Don't know how to handle partialDataset: " + datasetOptTimeContext.getText());
        }

        return ref[0];
    }

    public static DateTime parseDateTime(JQLParser.DateTimeContext dateTimeContext) {
        if (dateTimeContext.DATETIME_TOKEN() != null) {
            return new DateTime(dateTimeContext.DATETIME_TOKEN().getText().replaceAll(" ", "T"));
        } else if (dateTimeContext.DATE_TOKEN() != null) {
            return new DateTime(dateTimeContext.DATE_TOKEN().getText());
        } else if (dateTimeContext.STRING_LITERAL() != null) {
            return new DateTime(ParserCommon.unquote(dateTimeContext.STRING_LITERAL().getText()));
        } else if (dateTimeContext.FOUR_DIGIT_NUMBER() != null) {
            return new DateTime(dateTimeContext.FOUR_DIGIT_NUMBER().getText());
        } else if (dateTimeContext.timePeriod() != null) {
            final List<Pair<Integer, TimeUnit>> pairs = ParserCommon.parseTimePeriod(dateTimeContext.timePeriod());
            DateTime dt = DateTime.now().withTimeAtStartOfDay();
            for (final Pair<Integer, TimeUnit> pair : pairs) {
                dt = TimeUnit.subtract(dt, pair.getFirst(), pair.getSecond());
            }
            return dt;
        } else {
            final String textValue = dateTimeContext.getText();
            if ("yesterday".startsWith(textValue)) {
                return DateTime.now().withTimeAtStartOfDay().minusDays(1);
            } else if (textValue.length() >= 3 && "today".startsWith(textValue)) {
                return DateTime.now().withTimeAtStartOfDay();
            } else if (textValue.length() >= 3 && "tomorrow".startsWith(textValue)) {
                return DateTime.now().withTimeAtStartOfDay().plusDays(1);
            }
        }
        throw new UnsupportedOperationException("Don't know how to handle dateTime: " + dateTimeContext.getText());
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
