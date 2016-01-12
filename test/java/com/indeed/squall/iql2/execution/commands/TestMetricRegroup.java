package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Document;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TestUtil;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestMetricRegroup {

    public static final String SESSION = "session";
    public static final String FIELD = "stat";

    private static List<Document> datasetDocuments() {
        final ArrayList<Document> documents = new ArrayList<>();

        final long timestamp = new DateTime(2015, 1, 1, 0, 0).getMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                // TODO: Remove fakeField after fixing MemoryFlamdex.
                documents.add(Document.builder(SESSION, timestamp).addTerm(FIELD, i).addTerm("fakeField", 0L).build());
            }
        }

        return documents;
    }

    @Test
    public void testBasic() throws ImhotepOutOfMemoryException, IOException {
        try (final Closer closer = Closer.create()) {
            final Session session = TestUtil.buildSession(datasetDocuments(), new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0), closer);

            final MetricRegroup regroup = new MetricRegroup(ImmutableMap.of(SESSION, Collections.singletonList(FIELD)), 0, 5, 1, false, false);
            regroup.execute(session, new Consumer.NoOpConsumer<String>());

            final GetGroupStats getGroupStats = new GetGroupStats(Collections.<AggregateMetric>singletonList(new DocumentLevelMetric(SESSION, Collections.singletonList("1"))), false);
            final List<String> output = TestUtil.evaluateGroupStats(session, getGroupStats);

            final List<String> expected = Lists.newArrayList(
                    "0\t0",
                    "1\t1",
                    "2\t2",
                    "3\t3",
                    "4\t4",
                    "[-∞, 0)\t0",
                    "[5, ∞)\t" + (9 * 10 / 2 - 4 * 5 / 2)
            );

            Assert.assertEquals(expected, output);
        }
    }

    @Test
    public void testNoGutters() throws ImhotepOutOfMemoryException, IOException {
        try (final Closer closer = Closer.create()) {
            final Session session = TestUtil.buildSession(datasetDocuments(), new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0), closer);

            final MetricRegroup regroup = new MetricRegroup(ImmutableMap.of(SESSION, Collections.singletonList(FIELD)), 0, 5, 1, true, false);
            regroup.execute(session, new Consumer.NoOpConsumer<String>());

            final GetGroupStats getGroupStats = new GetGroupStats(Collections.<AggregateMetric>singletonList(new DocumentLevelMetric(SESSION, Collections.singletonList("1"))), false);
            final List<String> output = TestUtil.evaluateGroupStats(session, getGroupStats);

            final List<String> expected = Lists.newArrayList(
                    "0\t0",
                    "1\t1",
                    "2\t2",
                    "3\t3",
                    "4\t4"
            );

            Assert.assertEquals(expected, output);
        }
    }

    @Test
    public void testWithDefault() throws ImhotepOutOfMemoryException, IOException {
        try (final Closer closer = Closer.create()) {
            final Session session = TestUtil.buildSession(datasetDocuments(), new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0), closer);

            final MetricRegroup regroup = new MetricRegroup(ImmutableMap.of(SESSION, Collections.singletonList(FIELD)), 0, 5, 1, true, true);
            regroup.execute(session, new Consumer.NoOpConsumer<String>());

            final GetGroupStats getGroupStats = new GetGroupStats(Collections.<AggregateMetric>singletonList(new DocumentLevelMetric(SESSION, Collections.singletonList("1"))), false);
            final List<String> output = TestUtil.evaluateGroupStats(session, getGroupStats);

            final List<String> expected = Lists.newArrayList(
                    "0\t0",
                    "1\t1",
                    "2\t2",
                    "3\t3",
                    "4\t4",
                    "DEFAULT\t" + (9 * 10 / 2 - 4 * 5 / 2)
            );

            Assert.assertEquals(expected, output);
        }
    }

}
