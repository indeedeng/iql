package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QuantilesTest extends BasicTest {
    @Test
    public void test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 0.2)", "0", "9.5", "19"));
        expected.add(ImmutableList.of("[0.2, 0.4)", "20", "29.5", "39"));
        expected.add(ImmutableList.of("[0.4, 0.6)", "40", "49.5", "59"));
        expected.add(ImmutableList.of("[0.6, 0.8)", "60", "69.5", "79"));
        expected.add(ImmutableList.of("[0.8, 1.0)", "80", "89.5", "99"));
        QueryServletTestUtils.testIQL2(dataset(), expected, "from dataset yesterday today group by quantiles(f, 5) select field_min(f), f / count(), field_max(f)");
    }

    public static List<Shard> dataset() {
        final List<Shard> result = new ArrayList<>();
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        for (int i = 0; i < 100; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("f", i);
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, TIME_ZONE).getMillis() / 1000);
            doc.addIntTerm("fakeField", 0);
            flamdex.addDocument(doc);
        }

        result.add(new Shard("dataset", "index20150101", flamdex));

        return result;
    }
}
