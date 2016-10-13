package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExtractTest extends BasicTest {
    private static List<Shard> createDataset() {
        final MemoryFlamdex flamdex = new MemoryFlamdex();

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("field1", "a 2 5");
        doc1.addStringTerm("field2", "7");
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("field1", "a 3 2");
        doc2.addStringTerm("field2", "5");
        doc2.addStringTerm("field3", "10");
        flamdex.addDocument(doc2);

        return ImmutableList.of(new Shard("extract", "index20150101", flamdex));
    }

    @Test
    public void testBasic() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("5", "5", "3", "2", "10"));
        expected.add(ImmutableList.of("7", "7", "2", "5", "0"));
        QueryServletTestUtils.testIQL2(createDataset(), expected, "from extract yesterday today group by field2 select extract(field2, \"(\\\\d+)\"), extract(field1, \"a (\\\\d) (\\\\d)\"), extract(field1, \"a (\\\\d) (\\\\d)\", 2), extract(field3, \"(\\\\d+)\")");
    }
}
