package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.*;

public class FieldRegroupTest extends BasicTest {
    @Test
    public void testBasicGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("15", "1", "15"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc, allbit select count(), ojc");
    }

    @Test
    public void testFirstK() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc select count(), ojc LIMIT 3");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc, allbit select count(), ojc LIMIT 3");
    }

    @Test
    public void testImplicitOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("3", "60", "180"));
        // TODO: Introduce fully deterministic ordering for ties and increase to top 3?
//        expected.add(ImmutableList.of("0", "2", "0"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc[2] select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[2], allbit select count(), ojc");
    }

    // IF THIS BREAKS, READ THE TODO BEFORE TRYING TO FIGURE OUT WHAT YOU DID
    @Test
    public void testImplicitOrderingBackwards() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        // TODO: Determinism amongst ties? This will almost certainly break
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("2", "1", "2"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc[BOTTOM 3] select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[BOTTOM 3], allbit select count(), ojc");
    }

    @Test
    public void testTopKOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc[100 BY ojc/count()] select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[100 BY ojc/count()], allbit select count(), ojc");
    }

    @Test
    public void testTopKBottomOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("5", "1", "5"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc[BOTTOM 3 BY ojc] select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[BOTTOM 3 BY ojc], allbit select count(), ojc");
    }

    @Test
    public void testOrderingOnly() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by ojc[BY ojc/count()] select count(), ojc");
        testIQL2(OrganicDataset.create(), addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[BY ojc/count()], allbit select count(), ojc");
    }
}
