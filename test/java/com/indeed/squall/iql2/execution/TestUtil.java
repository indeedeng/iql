package com.indeed.squall.iql2.execution;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.math.DoubleMath;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.commands.GetGroupDistincts;
import com.indeed.squall.iql2.execution.commands.GetGroupStats;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.dimensions.DimensionDetails;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.util.core.TreeTimer;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Ignore
public class TestUtil {
    public static Session buildSession(List<Document> documents, DateTime start, DateTime end, Closer closer) throws ImhotepOutOfMemoryException {
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
            final ImhotepSession session = new ImhotepJavaLocalSession(entry.getValue());
            sessionInfoMap.put(entry.getKey(), new Session.ImhotepSessionInfo(session, dimensions, datasetIntFields.get(entry.getKey()), datasetStringFields.get(entry.getKey()), start, end, "unixtime"));
        }

        return new Session(sessionInfoMap, new TreeTimer(), new NoOpProgressCallback(), null);
    }

    public static void testOne(final List<Document> documents, final List<Command> commands, DateTime start, DateTime end) throws IOException, ImhotepOutOfMemoryException {
        try (final Closer closer = Closer.create()) {
            final Session session = buildSession(documents, start, end, closer);
            final SimpleSession simpleSession = new SimpleSession(documents, start, end);

            final List<Command> verificationCommands = makeVerificationCommands(documents);

            verify(session, simpleSession, verificationCommands);

            for (final Command command : commands) {
                testCommand(session, simpleSession, command);
                verify(session, simpleSession, verificationCommands);
            }
        }
    }

    private static void verify(Session session, SimpleSession simpleSession, List<Command> verificationCommands) throws ImhotepOutOfMemoryException, IOException {
        for (final Command command : verify(verificationCommands)) {
            System.out.println("command = " + command);
            testCommand(session, simpleSession, command);
        }
    }

    private static List<Command> verify(List<Command> verificationCommands) {
        return verificationCommands;
    }

    private static void testCommand(Session session, SimpleSession simpleSession, Command command) throws ImhotepOutOfMemoryException, IOException {
        System.out.println("Running " + command);

        final SavingConsumer<String> out1 = new SavingConsumer<>();
        command.execute(session, out1);

        final SavingConsumer<String> out2 = new SavingConsumer<>();
        simpleSession.handleCommand(command, out2);

        final List<String> results1 = out1.getElements();
        final List<String> results2 = out2.getElements();
        Assert.assertEquals(results2, results1);

        System.out.println(results1);
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
        commands.add(new GetGroupStats(metrics, Collections.nCopies(metrics.size(), Optional.<String>absent()), false));
        return commands;
    }

    public static List<String> evaluateGroupStats(Session session, GetGroupStats getGroupStats) throws ImhotepOutOfMemoryException {
        final List<Session.GroupStats> groupStatses = getGroupStats.evaluate(session);
        final List<String> output = new ArrayList<>();
        for (final Session.GroupStats groupStats : groupStatses) {
            final ArrayList<String> groupKey = new ArrayList<>();
            session.groupKeySet.groupKey(groupStats.group).addToList(groupKey);
            if (groupKey.isEmpty()) {
                output.add(makeNumber(groupStats.stats[0]));
            } else {
                output.add(groupKey.get(0)+"\t"+makeNumber(groupStats.stats[0]));
            }
        }
        return output;
    }

    private static String makeNumber(double v) {
        if (DoubleMath.isMathematicalInteger(v)) {
            return String.format("%.0f", v);
        } else {
            return String.valueOf(v);
        }
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
