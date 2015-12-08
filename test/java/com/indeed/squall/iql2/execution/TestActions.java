package com.indeed.squall.iql2.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.actions.Action;
import com.indeed.squall.iql2.execution.actions.IntOrAction;
import com.indeed.squall.iql2.execution.actions.MetricAction;
import com.indeed.squall.iql2.execution.actions.RegexAction;
import com.indeed.squall.iql2.execution.actions.SampleAction;
import com.indeed.squall.iql2.execution.actions.StringOrAction;
import com.indeed.squall.iql2.execution.commands.ApplyFilterActions;
import com.indeed.squall.iql2.execution.commands.Command;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Must keep all documents in groups {0, 1}, because if done otherwise it breaks invariants that hold in normal execution.
 *
 * The main invariant being that ApplyFilterActions starts with all documents in groups {0, 1} and ends with them in
 * {0, 1} and thus does not need to do any updates to GroupKeySets and whatnot.
 */
public class TestActions {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    @Test
    public void testIntOrAction() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis()).addTerm("field", (long) i).build());
        }

        final List<Command> commands = new ArrayList<>();

        // Mis-targeted action that does nothing.
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.<String>emptySet(), "field", Collections.<Long>emptySet(), 1, 0, 0))));

        // Real actions
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.singleton("organic"), "field", ImmutableSet.of(1L, 3L, 7L, 50L, 125L), 1, 1, 0))));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new IntOrAction(Collections.singleton("organic"), "field", ImmutableSet.of(1L, 3L), 1, 1, 0))));

        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testStringOrAction() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis()).addTerm("intfield", (long) i).addTerm("strfield", String.valueOf(i)).build());
        }

        final List<Command> commands = new ArrayList<>();

        // Mis-targeted action that does nothing.
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.<String>emptySet(), "strfield", Collections.<String>emptySet(), 1, 0, 0))));

        // Real actions
        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.singleton("organic"), "strfield", ImmutableSet.of("1", "3", "7", "50", "125"), 1, 1, 0))));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(new StringOrAction(Collections.singleton("organic"), "strfield", ImmutableSet.of("1", "3"), 1, 1, 0))));

        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
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

        final List<Command> commands = new ArrayList<>();

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        Collections.singleton("organic"),
                        ImmutableMap.of("organic", Collections.singletonList("divBy3")),
                        1, 1, 0
                )
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        Collections.singleton("sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("divBy5")),
                        1, 1, 0
                )
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        ImmutableSet.of("organic", "sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("divBy7"), "organic", Collections.singletonList("divBy7")),
                        1, 1, 0
                )
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new MetricAction(
                        ImmutableSet.of("organic", "sponsored"),
                        ImmutableMap.of("sponsored", Collections.singletonList("0"), "organic", Collections.singletonList("0")),
                        1, 1, 0
                )
        )));

        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testRegexAction() throws Exception {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i <= 100; i++) {
            final Document.Builder doc = Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis());
            doc.addTerm("string", String.valueOf(i));
            // TODO: Make 'int' an int field after making MemoryFlamdex::getStringTermIterator() work on int fields.
            doc.addTerm("int", String.valueOf(i));
            documents.add(doc.build());
        }

        final List<Command> commands = new ArrayList<>();

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new RegexAction(
                        Collections.singleton("organic"),
                        "string",
                        ".*1.*",
                        1, 1, 0
                )
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new RegexAction(
                        Collections.singleton("organic"),
                        "int",
                        ".*2.*",
                        1, 1, 0
                )
        )));

        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testSampleAction() throws Exception {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i <= 100; i++) {
            final Document.Builder doc = Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis());
            doc.addTerm("string", String.valueOf(i));
            // TODO: Make 'int' an int field after making MemoryFlamdex::getStringTermIterator() work on int fields.
            doc.addTerm("int", String.valueOf(i));
            documents.add(doc.build());
        }

        final List<Command> commands = new ArrayList<>();

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new SampleAction(Collections.singleton("organic"), "string", 0.5, "abcdef", 1, 1, 0)
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new SampleAction(Collections.singleton("organic"), "int", 0.5, "abcdefghijk", 1, 1, 0)
        )));

        commands.add(new ApplyFilterActions(Collections.<Action>singletonList(
                new SampleAction(Collections.singleton("organic"), "string", 0.25, "once more with feeling", 1, 1, 0)
        )));

        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }
}
