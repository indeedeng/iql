package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RegexTest {
    @Test
    public void testNormalDocFilter1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"a\" select count()", true);
    }

    @Test
    public void testNormalDocFilter2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"(b|c)\" select count()", true);
    }

    @Test
    public void testNormalDocMetric1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select tk =~ \"a\"", true);
    }

    @Test
    public void testNormalDocMetric2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select tk =~ \"(b|c)\"", true);
    }

    @Test
    public void testAggregateDocFilter() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having tk =~ \"(a|b|c)\" > 0 select count()");
    }

    @Test
    public void testInvalid() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        try {
            QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"[]*\" select count()", true);
            Assert.fail("Regex should not have parsed successfully.");
        } catch (Exception e) {
        }
    }

    @Test
    public void testExpensive() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        try {
            QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \".*ios.*|.*software.*|.*web.*|.*java.*|.*hadoop.*|.*spark.*|.*nlp.*|.*algorithm.*|.*python.*|.*matlab.*|.*swift.*|.*android.*\" select count()", true);
            Assert.fail("Regex should not have parsed successfully.");
        } catch (Exception e) {
        }
    }
}
