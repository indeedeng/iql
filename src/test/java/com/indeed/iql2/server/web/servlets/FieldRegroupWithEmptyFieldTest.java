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

package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class FieldRegroupWithEmptyFieldTest extends BasicTest {
    final Dataset dataset = createDataset();

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    @Test
    public void testGroupByIntLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "4"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by i1 limit 2", true);
    }

    @Test
    public void testGroupByMultiLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("1", "2", "1"));
        expected.add(ImmutableList.of("2", "2", "2"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by i1, i2 limit 3", true);
    }

    @Test
    public void testGroupByStrLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1"));
        expected.add(ImmutableList.of("b", "4"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by s1 limit 2", true);
    }

    @Test
    public void testGroupByStrMultiLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "a", "1"));
        expected.add(ImmutableList.of("a", "b", "1"));
        expected.add(ImmutableList.of("b", "b", "2"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by s1, s2 limit 3", true);
    }

    @Test
    public void testGroupByImplicitLimitWithOrder() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "2"));
        expected.add(ImmutableList.of("2", "4"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by i1[by i2] limit 2", true);
    }

    @Test
    public void testGroupByAggLimitWithOrder() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testAll(dataset, expected, "from regroupEmptyField yesterday today group by i1[by i1+i1] limit 2", true);
    }

    @Test
    public void testGroupImplicitWithOrderLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1[2] limit 3", true);
    }

    @Test
    public void testGroupByAggreWithOrderLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("4", "1"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1[5 BY -i1] LIMIT 3", true);
    }

    @Test
    public void testGroupByImplicitLimitWithHaving() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1 having count() > 1 limit 2", true);
    }

    @Test
    public void testGroupByImplicitOrderLimitWithHaving() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "2"));
        expected.add(ImmutableList.of("2", "4"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1[bottom 5] having count() > 1 limit 2", true);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("3", "2"), ImmutableList.of("2", "4")), "from regroupEmptyField yesterday today group by i1[bottom 5] having count() > 1 limit 2", true);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("3", "2")), "from regroupEmptyField yesterday today group by i1[1 by i2] limit 2", true);
    }

    @Test
    public void testGroupByMultipleWithOrderLimitStream() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("2", "2", "2"));
        expected.add(ImmutableList.of("3", "3", "2"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1[2 by i2], i2 limit 3", true);
    }

    @Test
    public void testGroupByMultipleWithOrderLimitNonStream() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "3", "2"));
        expected.add(ImmutableList.of("2", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        testIQL2(dataset, expected, "from regroupEmptyField yesterday today group by i1[2 by i2], i2[2] limit 3", true);
    }

    // fields [time, s1, s2, i1, i2]
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 12), 0, 1, "", "a"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 13), 0, 2, "", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 15), 1, 2, "a", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30), 2, 0, "b", ""));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30), 2, 1, "b", "a"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 40), 2, 2, "b", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 45), 2, 2, "b", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 48), 3, 3, "c", "c"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 48), 3, 3, "c", "c"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 58), 4, 3, "d", "c"));
        shards.add(new Dataset.DatasetShard("regroupEmptyField", "index20150101.00", flamdex));
        return new Dataset(shards);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int i1, int i2, String s1, String s2) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        if (i1 > 0) {
            doc.addIntTerm("i1", i1);
        }
        if (i2 > 0) {
            doc.addIntTerm("i2", i2);
        }
        if (!s1.equals("")) {
            doc.addStringTerm("s1", s1);
        }
        if (!s2.equals("")) {
            doc.addStringTerm("s2", s2);
        }
        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);
        return doc;
    }
}
