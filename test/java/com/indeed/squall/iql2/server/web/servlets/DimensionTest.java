package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

/**
 *
 */
public class DimensionTest {

    @Test
    public void testSelect() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "0", "5", "200", "205", "10", "205", "1"));
        testAll(createDataset(), expected, "from dimension yesterday today SELECT empty, same, calc, combined, aliasi1, aliasCombined, i1divi2");
    }

    @Test
    public void testSelectMultiple() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "200", "100", "300", "1"));
        testIQL2(createDataset(), expected, "from dimension 2015-01-01 2015-01-02 as d1, dimension 2015-01-02 2015-01-03 as d2 SELECT d1.calc, d2.calc, calc, d1.i1divi2");
    }

    @Test
    public void testIllegal() throws Exception {
        try {
            testAll(createDataset(), Collections.emptyList(), "from dimension yesterday today SELECT i1+i1divi2");
            Assert.fail("Result of AggregateMetric Dimension like / can't be used as input for further calculations");
        } catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "2"));
        expected.add(ImmutableList.of("3", "2", "2"));
        expected.add(ImmutableList.of("4", "6", "1"));
        testAll(createDataset(), expected, "from dimension yesterday today GROUP BY i1 SELECT i2, counts");
    }

    public static List<Shard> createDataset() {
        final List<Shard> result = new ArrayList<>();

        {
            final MemoryFlamdex flamdex = new MemoryFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 12), 0, 1, ""));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 13), 0, 1, ""));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 1, 0, 15), 3, 2, "a"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, 0, 30), 3, 0, "b"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 4, 0, 30), 4, 6, "b"));
            result.add(new Shard("dimension", "index20150101.00", flamdex));
        }

        {
            final MemoryFlamdex flamdex = new MemoryFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 2, 2, 0, 30), 4, 6, "b"));
            result.add(new Shard("dimension", "index20150102.00", flamdex));
        }

        return result;
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int i1, int i2, String s1) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("i1", i1);
        doc.addIntTerm("i2", i2);
        doc.addStringTerm("s1", s1);
        doc.addIntTerm("empty", 0);
        doc.addIntTerm("same", 1);
        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);
        return doc;
    }

}
