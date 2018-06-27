package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
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

    // Behavior changed in IQL-610 to make empty SELECT act as SELECT COUNT()
    @Test
    public void emptySelectIsCountSimple() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where group by select");
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by select");
        testAll(OrganicDataset.create(), expected, "select from organic yesterday today");
        testAll(OrganicDataset.create(), expected, "select from organic yesterday today where");
        testAll(OrganicDataset.create(), expected, "select from organic yesterday today where group by");
   }

    @Test
    public void selectFromAliased() throws Exception {
        // This test is not strictly about empty clauses, but
        // about disambiguating between empty select (select from)
        // and select from where from is an aliased field name
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "2653"));
        testAll(OrganicDataset.create(), expected, "select from from organic yesterday today aliasing (oji as from)", true);
    }

    @Test
    public void emptySelectIsCountWithGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where group by tk select");
    }

    @Test
    public void emptySelectIsCountWithWhere() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "129"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where oji = 10 select");

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "141"));
        testAll(OrganicDataset.create(), expected2, "from organic yesterday today where tk = 'd' select");
    }

    @Test
    public void emptySelectIsCountWithWhereAndGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "1"));
        expected.add(ImmutableList.of("c", "3"));
        expected.add(ImmutableList.of("d", "121"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today where oji = 10 group by tk select");
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
