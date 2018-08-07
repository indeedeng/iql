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

package com.indeed.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.Document;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TestUtil;
import com.indeed.iql2.execution.compat.Consumer;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.metrics.aggregate.Constant;
import com.indeed.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestApplyGroupFilter {

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

            final MetricRegroup metricRegroup = new MetricRegroup(ImmutableMap.of(SESSION, Collections.singletonList(FIELD)), 0, 10, 1, true, false, false);
            metricRegroup.execute(session, new Consumer.NoOpConsumer<String>());

            final ApplyGroupFilter applyGroupFilter = new ApplyGroupFilter(new AggregateFilter.GreaterThan(new DocumentLevelMetric(SESSION, Arrays.asList(FIELD, "4", ">")), new Constant(0)));
            applyGroupFilter.execute(session, new Consumer.NoOpConsumer<String>());

            final GetGroupStats getGroupStats = new GetGroupStats(Collections.<AggregateMetric>singletonList(new DocumentLevelMetric(SESSION, Collections.singletonList("2"))), Collections.singletonList(Optional.<String>absent()), false);
            final List<String> output = TestUtil.evaluateGroupStats(session, getGroupStats);

            final List<String> expected = Lists.newArrayList(
                    "[5, 6)\t10",
                    "[6, 7)\t12",
                    "[7, 8)\t14",
                    "[8, 9)\t16",
                    "[9, 10)\t18"
            );

            Assert.assertEquals(expected, output);
        }
    }
}
