package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

// An important note about this test is that BOTH of the below test cases are broken.
// Neither one is right, and we do not currently have a way to get the "right" answer.
// The string (iql1) answer is missing all of the int values.
// The int (iql2) answer is missing all of the string values.
public class ConflictFieldTest extends BasicTest {
    @Test
    public void testIQL1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("A", "1"));
        expected.add(ImmutableList.of("B", "1"));
        expected.add(ImmutableList.of("C", "1"));
        QueryServletTestUtils.testIQL1(expected, "from conflict yesterday today group by fieldName", true);
    }

    @Test
    public void testIQL2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "1"));
        QueryServletTestUtils.testIQL2(expected, "from conflict yesterday today group by fieldName", true);
    }
}
