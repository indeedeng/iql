package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class FieldRegroupWithEmptyFieldTest extends BasicTest {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    @Test
    public void testGroupByIntLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "4"));
        testAll(createDataset(), expected, "from organic yesterday today group by i1 limit 2");
    }

    @Test
    public void testGroupByMultiLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("1", "2", "1"));
        expected.add(ImmutableList.of("2", "2", "2"));
        testAll(createDataset(), expected, "from organic yesterday today group by i1, i2 limit 3");
    }

    @Test
    public void testGroupByStrLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1"));
        expected.add(ImmutableList.of("b", "4"));
        testAll(createDataset(), expected, "from organic yesterday today group by s1 limit 2");
    }

    @Test
    public void testGroupByStrMultiLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "a", "1"));
        expected.add(ImmutableList.of("a", "b", "1"));
        expected.add(ImmutableList.of("b", "b", "2"));
        testAll(createDataset(), expected, "from organic yesterday today group by s1, s2 limit 3");
    }

    @Test
    public void testGroupByImplicitLimitWithOrder() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "2"));
        expected.add(ImmutableList.of("2", "4"));
        testAll(createDataset(), expected, "from organic yesterday today group by i1[by i2] limit 2");
    }

    @Test
    public void testGroupByAggLimitWithOrder() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testAll(createDataset(), expected, "from organic yesterday today group by i1[by i1+i1] limit 2");
    }

    @Test
    public void testGroupImplicitWithOrderLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(createDataset(), expected, "from organic yesterday today group by i1[2] limit 3");
    }

    @Test
    public void testGroupByAggreWithOrderLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("4", "1"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(createDataset(), expected, "from organic yesterday today group by i1[5 BY -i1] LIMIT 3");
    }

    @Test
    public void testGroupByImplicitLimitWithHaving() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "4"));
        expected.add(ImmutableList.of("3", "2"));
        testIQL2(createDataset(), expected, "from organic yesterday today group by i1 having count() > 1 limit 2");
    }

    @Test
    public void testGroupByImplicitOrderLimitWithHaving() throws Exception {
        testIQL2(createDataset(), ImmutableList.of(ImmutableList.of("3", "2"), ImmutableList.of("2", "4")), "from organic yesterday today group by i1[bottom 5] having count() > 1 limit 2");
        testIQL2(createDataset(), ImmutableList.of(ImmutableList.of("3", "2")), "from organic yesterday today group by i1[1 by i2] limit 2");
    }


    @Test
    public void testGroupByMultipleWithOrderLimitStream() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("2", "2", "2"));
        expected.add(ImmutableList.of("3", "3", "2"));
        testIQL2(createDataset(), expected, "from organic yesterday today group by i1[2 by i2], i2 limit 3");
    }

    @Test
    public void testGroupByMultipleWithOrderLimitNonStream() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "3", "2"));
        expected.add(ImmutableList.of("2", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        testIQL2(createDataset(), expected, "from organic yesterday today group by i1[2 by i2], i2[2] limit 3");
    }

    // fields [time, s1, s2, i1, i2]
    public static List<Shard> createDataset() {
        final List<Shard> result = new ArrayList<>();

        final MemoryFlamdex flamdex = new MemoryFlamdex();
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
        result.add(new Shard("organic", "index20150101.00", flamdex));

        return result;
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
