package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import org.junit.Test;

import java.util.List;

/**
 * @author zheli
 */

public class LuceneTest {

    @Test
    public void testBasic() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"tk:a\")");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select lucene(\"tk:b\")");
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"tk:a\")");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select lucene(\"tK:b\")");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"Tk:c\")");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "143")), "from organic yesterday today select lucene(\"tk:d OR Tk:b\")");
    }
}
