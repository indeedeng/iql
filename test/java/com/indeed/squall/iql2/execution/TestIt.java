package com.indeed.squall.iql2.execution;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.metrics.Count;
import com.indeed.squall.iql2.execution.actions.Action;
import com.indeed.squall.iql2.execution.actions.IntOrAction;
import com.indeed.squall.iql2.execution.actions.MetricAction;
import com.indeed.squall.iql2.execution.actions.StringOrAction;
import com.indeed.squall.iql2.execution.commands.ApplyFilterActions;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.commands.GetGroupDistincts;
import com.indeed.squall.iql2.execution.commands.GetGroupStats;
import com.indeed.squall.iql2.execution.commands.TimePeriodRegroup;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.dimensions.DimensionDetails;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.util.core.TreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestIt {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public void testOne(final List<Document> documents, final List<Command> commands, DateTime start, DateTime end) throws IOException, ImhotepOutOfMemoryException {
        try (final Closer closer = Closer.create()) {
            final Map<String, MemoryFlamdex> datasetFlamdexes = new HashMap<>();
            final Map<String, Set<String>> datasetIntFields = new HashMap<>();
            final Map<String, Set<String>> datasetStringFields = new HashMap<>();
            for (final Document document : documents) {
                if (!datasetFlamdexes.containsKey(document.dataset)) {
                    datasetFlamdexes.put(document.dataset, closer.register(new MemoryFlamdex()));
                    datasetIntFields.put(document.dataset, new HashSet<String>());
                    datasetStringFields.put(document.dataset, new HashSet<String>());
                }
                datasetFlamdexes.get(document.dataset).addDocument(document.asFlamdex());
                datasetIntFields.get(document.dataset).addAll(document.intFields.keySet());
                datasetStringFields.get(document.dataset).addAll(document.stringFields.keySet());
            }
            final DatasetDimensions dimensions = new DatasetDimensions(ImmutableMap.<String, DimensionDetails>of());
            final Map<String, Session.ImhotepSessionInfo> sessionInfoMap = new HashMap<>();
            for (final Map.Entry<String, MemoryFlamdex> entry : datasetFlamdexes.entrySet()) {
                final ImhotepSession session = new ImhotepLocalSession(entry.getValue());
                sessionInfoMap.put(entry.getKey(), new Session.ImhotepSessionInfo(session, dimensions, datasetIntFields.get(entry.getKey()), datasetStringFields.get(entry.getKey()), start, end, "unixtime"));
            }

            final Session session = new Session(sessionInfoMap, new TreeTimer(), new NoOpProgressCallback());
            final SimpleSession simpleSession = new SimpleSession(documents, start, end);

            for (final Command command : commands) {
                System.out.println("Running " + command);

                final SavingConsumer<String> out1 = new SavingConsumer<>();
                command.execute(session, out1);

                final SavingConsumer<String> out2 = new SavingConsumer<>();
                simpleSession.handleCommand(command, out2);

                final List<String> results1 = out1.getElements();
                final List<String> results2 = out2.getElements();
                Assert.assertEquals(results2, results1);

                System.out.println(results1);

                // TODO: Do some non-effectful operations to measure more equivalences.
            }
        }
    }

    private static List<Command> makeVerificationCommands(List<Document> documents) {
        final Map<String, Set<String>> datasetIntFields = new HashMap<>();
        final Map<String, Set<String>> datasetStringFields = new HashMap<>();
        for (final Document document : documents) {
            if (!datasetIntFields.containsKey(document.dataset)) {
                datasetIntFields.put(document.dataset, new HashSet<String>());
                datasetStringFields.put(document.dataset, new HashSet<String>());
            }
            datasetIntFields.get(document.dataset).addAll(document.intFields.keySet());
            datasetStringFields.get(document.dataset).addAll(document.stringFields.keySet());
        }
        final List<Command> commands = new ArrayList<>();
        final List<AggregateMetric> metrics = new ArrayList<>();
        for (final String dataset : datasetIntFields.keySet()) {
            metrics.add(new DocumentLevelMetric(dataset, Collections.singletonList("count()")));
            for (final String field : datasetIntFields.get(dataset)) {
                metrics.add(new DocumentLevelMetric(dataset, Collections.singletonList(field)));
            }
            for (final String field : datasetStringFields.get(dataset)) {
                commands.add(new GetGroupDistincts(Collections.singleton(dataset), field, Optional.<AggregateFilter>absent(), 1));
            }
        }
        commands.add(new GetGroupStats(metrics, false));
        return commands;
    }

    @Test
    public void letsDoIt() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        documents.add(
            Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis())
                    .build()
        );
        final List<Command> verificationCommands = makeVerificationCommands(documents);
        final List<Command> commands = new ArrayList<>();
        commands.addAll(verificationCommands);
        testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testGroupByTime() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, i, 0).getMillis()).build());
        }
        final List<Command> verificationCommands = makeVerificationCommands(documents);
        final List<Command> commands = new ArrayList<>();
        commands.addAll(verificationCommands);
        commands.add(new TimePeriodRegroup(1000 * 60 * 60 /* 1 h */, Optional.<String>absent(), Optional.<String>absent()));
        commands.addAll(verificationCommands);
        testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testIntOrAction() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis()).addTerm("field", (long) i).build());
        }

        final List<Command> verificationCommands = makeVerificationCommands(documents);

        final List<Command> commands = new ArrayList<>();
        commands.addAll(verificationCommands);

        // Mis-targeted action that does nothing.
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.<String>emptySet(), "field", Collections.<Long>emptySet(), 1, 0, 0))));
        commands.addAll(verificationCommands);

        // Real actions
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.singleton("organic"), "field", ImmutableSet.of(1L, 3L, 7L, 50L, 125L), 1, 1, 0))));
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.singleton("organic"), "field", ImmutableSet.of(1L, 3L), 1, 1, 0))));
        commands.addAll(verificationCommands);

        testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testStringOrAction() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis()).addTerm("intfield", (long) i).addTerm("strfield", String.valueOf(i)).build());
        }

        final List<Command> verificationCommands = makeVerificationCommands(documents);

        final List<Command> commands = new ArrayList<>();
        commands.addAll(verificationCommands);

        // Mis-targeted action that does nothing.
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.<String>emptySet(), "strfield", Collections.<String>emptySet(), 1, 0, 0))));
        commands.addAll(verificationCommands);

        // Real actions
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.singleton("organic"), "strfield", ImmutableSet.of("1", "3", "7", "50", "125"), 1, 1, 0))));
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.singleton("organic"), "strfield", ImmutableSet.of("1", "3"), 1, 1, 0))));
        commands.addAll(verificationCommands);

        testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testMetricAction() throws Exception {
        final List<Document> documents = new ArrayList<>();
        for (final String dataset : Arrays.asList("organic", "sponsored")) {
            for (int i = 0; i < 100; i++) {
                final Document.Builder doc = Document.builder(dataset, new DateTime(2015, 1, 1, 0, 0).getMillis());
                doc.addTerm("divBy3", (i % 3 == 0) ? 1 : 0);
                doc.addTerm("divBy5", (i % 5 == 0) ? 1 : 0);
                doc.addTerm("divBy7", (i % 7 == 0) ? 1 : 0);
                doc.addTerm("divBy11", (i % 11 == 0) ? 1 : 0);
                doc.addTerm("divBy13", (i % 13 == 0) ? 1 : 0);
                documents.add(doc.build());
            }
        }

        final List<Command> verificationCommands = makeVerificationCommands(documents);

        final List<Command> commands = new ArrayList<>();
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        Collections.singleton("organic"),
                        ImmutableMap.of("organic", Collections.singletonList("divBy3")),
                        1, 1, 0
                )
        )));
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        Collections.singleton("sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("divBy5")),
                        1, 1, 0
                )
        )));
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        ImmutableSet.of("organic", "sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("divBy7"), "organic", Collections.singletonList("divBy7")),
                        1, 1, 0
                )
        )));
        commands.addAll(verificationCommands);

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        ImmutableSet.of("organic", "sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("0"), "organic", Collections.singletonList("0")),
                        1, 1, 0
                )
        )));
        commands.addAll(verificationCommands);

        testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    public static class SavingConsumer<T> implements Consumer<T> {
        private final List<T> ts = new ArrayList<>();

        @Override
        public void accept(T t) {
            ts.add(t);
        }

        public List<T> getElements() {
            return Collections.unmodifiableList(ts);
        }

        public void clear() {
            ts.clear();
        }
    }
}
