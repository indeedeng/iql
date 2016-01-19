package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldInTermsTest {
    @Test
    public void testStringFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "145", "2"));

        for (final boolean stream : new boolean[]{false, true}) {
            Assert.assertEquals(expected, QueryServletTestUtils.runQuery(OrganicDataset.create(), "from organic yesterday today group by tk in ('a', \"b\") with default select count(), distinct(tk)", QueryServletTestUtils.LanguageVersion.IQL2, stream));
        }
    }

    @Test
    public void testIntFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "3"));
        expected.add(ImmutableList.of("10", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "65", "4"));

        for (final boolean stream : new boolean[]{false, true}) {
            Assert.assertEquals(expected, QueryServletTestUtils.runQuery(OrganicDataset.create(), "from organic yesterday today group by ojc in (1, 10) with default select count(), distinct(tk)", QueryServletTestUtils.LanguageVersion.IQL2, stream));
        }
    }
}
