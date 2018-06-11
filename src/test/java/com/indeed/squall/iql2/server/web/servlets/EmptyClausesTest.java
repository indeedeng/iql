package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author jwolfe
 */

public class EmptyClausesTest extends BasicTest {
    @Test
    public void emptyVariations() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where group by");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where group by select count()");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where select count()");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today select count()");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by select count()");
    }

    // NOTE: This behavior can and maybe should change. But it should be done in a separate
    //       ticket with separate discussion and external communication.
    @Test
    public void emptySelect() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        testAll(OrganicDataset.create(), expected, "from organic yesterday today select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where group by select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by select");
    }

    @Test
    public void testGroupBySelect() throws Exception {
        final List<Dataset.DatasetShard> dataset = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        flamdex.addDocument(new FlamdexDocument(
                Collections.emptyMap(),
                ImmutableMap.of(
                    "select",
                    ImmutableList.of("a")
                )
        ));
        flamdex.addDocument(new FlamdexDocument(
                Collections.emptyMap(),
                ImmutableMap.of(
                    "select",
                    ImmutableList.of("b")
                )
        ));
        dataset.add(new Dataset.DatasetShard("organic", "index20150101", flamdex));
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("a", "1"),
                ImmutableList.of("b", "1")
        );
        testAll(new Dataset(dataset), expected, "from organic yesterday today group by `select`");
    }
}
