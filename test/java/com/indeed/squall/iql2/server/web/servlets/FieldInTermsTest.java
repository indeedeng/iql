package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.withoutLastColumn;

public class FieldInTermsTest extends BasicTest {
    final Dataset dataset = OrganicDataset.create();

    @Test
    public void testStringFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "145", "2"));
        testIQL2(dataset, expected, "from organic yesterday today group by tk in ('a', \"b\") with default select count(), distinct(tk)", false);
        testIQL2(dataset, withoutLastColumn(expected), "from organic yesterday today group by tk in ('a', \"b\") with default select count()", false);
    }

    @Test
    public void testStringFieldNotInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "4", "1"));
        expected.add(ImmutableList.of("d", "141", "1"));
        expected.add(ImmutableList.of("DEFAULT", "6", "2"));
        testIQL2(dataset, expected, "from organic yesterday today group by tk not in ('a', \"b\") with default select count(), distinct(tk)", false);
        testIQL2(dataset, withoutLastColumn(expected), "from organic yesterday today group by tk not in ('a', \"b\") with default select count()", false);
    }

    @Test
    public void testIntFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "3"));
        expected.add(ImmutableList.of("10", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "65", "4"));
        testIQL2(dataset, expected, "from organic yesterday today group by ojc in (1, 10) with default select count(), distinct(tk)", false);
        testIQL2(dataset, withoutLastColumn(expected), "from organic yesterday today group by ojc in (1, 10) with default select count()", false);
    }

    @Test
    public void testIntFieldNotInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("3", "60", "1"));
        expected.add(ImmutableList.of("5", "1", "1"));
        expected.add(ImmutableList.of("15", "1", "1"));
        expected.add(ImmutableList.of("DEFAULT", "86", "3"));
        testIQL2(dataset, expected, "from organic yesterday today group by ojc not in (1, 10) with default select count(), distinct(tk)", false);
        testIQL2(dataset, withoutLastColumn(expected), "from organic yesterday today group by ojc not in (1, 10) with default select count()", false);
    }
}
