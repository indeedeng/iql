package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * These tests are to ensure that query hashes are different for queries that looks similar but different.
 * These tests relying on the fact that we use {@link CollisionCheckingQueryCache}.
 */
public class SimilarQueryTest {
    @Test
    public void testStringFieldIn() throws Exception {
        testAll(
                ImmutableList.of(ImmutableList.of("", "2")),
                "from countries yesterday today where country in ('au', 'co')"
        );
        testAll(
                ImmutableList.of(ImmutableList.of("", "0")),
                "from countries yesterday today where country in ('au, co')"
        );
    }

    @Test
    public void testSubsetFTGS() throws Exception {
        testAll(
                ImmutableList.of(ImmutableList.of("au", "1"), ImmutableList.of("co", "1")),
                "from countries yesterday today group by country in ('au', 'co')"
        );
        testAll(
                ImmutableList.of(),
                "from countries yesterday today group by country in ('au, co')"
        );
    }

    @Test
    public void testGroupByFieldIn() throws Exception {
        testAll(
                ImmutableList.of(ImmutableList.of("au", "[1, 2)", "1"), ImmutableList.of("co", "[1, 2)", "1")),
                "from countries yesterday today group by country in ('au', 'co'), bucket(count(), 1, 2, 1, 1)"
        );
        testAll(
                ImmutableList.of(),
                "from countries yesterday today group by country in ('au, co'), bucket(count(), 1, 2, 1, 1)"
        );
    }
}
