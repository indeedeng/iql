package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FloatScaleTest {
    private static List<Shard> createDataset() {
        final MemoryFlamdex flamdex = new MemoryFlamdex();

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("field1", "100.0");
        doc1.addStringTerm("field2", "100.000000");
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("field1", "0.001");
        doc2.addStringTerm("field2", "1000000");
        flamdex.addDocument(doc2);

        return ImmutableList.of(new Shard("floatscaletest", "index20150101", flamdex));
    }

    @Test
    public void testBasic() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100", "100", "100001", "100001", "999100"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from floatscaletest yesterday today select floatscale(field1, 1, 0), floatscale(field1), floatscale(field1, 1000, 0), floatscale(field1, 1000), floatscale(field2, 1, -500)");
    }
}
