package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset.DatasetFlamdex;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TSVEscapingTest extends BasicTest {
    private static Dataset createDataset() {
        final DatasetFlamdex flamdex = new DatasetFlamdex();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHours(-6));

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("sField", "Crazy\nTerm\t!\n\r");
        doc1.addIntTerm("iField", 3);
        doc1.addIntTerm("unixtime", DateTime.parse("2015-01-01", dateTimeFormatter).getMillis() / 1000);
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("sField", "NormalTerm");
        doc2.addIntTerm("iField", 2);
        doc2.addIntTerm("unixtime", DateTime.parse("2015-01-01", dateTimeFormatter).getMillis() / 1000);
        flamdex.addDocument(doc2);

        return new Dataset(ImmutableList.of(new Dataset.DatasetShard("tsvescape", "index20150101", flamdex)));
    }

    @Test
    public void testStreamingLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "1"));
        expected.add(ImmutableList.of("NormalTerm", "1"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from tsvescape yesterday today group by sField");
    }

    @Test
    public void testStreamingPreviousRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("NormalTerm", "2", "1"));
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "3", "1"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from tsvescape yesterday today group by sField, iField");
    }

    @Test
    public void testStreamingPreviousRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "1"));
        expected.add(ImmutableList.of("NormalTerm", "NormalTerm", "1"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from tsvescape yesterday today group by sField, sField");
    }

    @Test
    public void testGetGroupStats() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "1"));
        expected.add(ImmutableList.of("NormalTerm", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "1"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from tsvescape yesterday today group by sField, time(1d)");
    }
}
