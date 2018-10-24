package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class GroupByAliasedFieldTest {
    @Test
    public void groupByAliasLastGroupBy() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
                ImmutableList.of("abc", "3"),
                ImmutableList.of("abd", "1"),
                ImmutableList.of("bcd", "2"),
                ImmutableList.of("cn", "1"),
                ImmutableList.of("ddd", "1"),
                ImmutableList.of("gb", "2"),
                ImmutableList.of("jp", "2"),
                ImmutableList.of("pqr", "1"),
                ImmutableList.of("uk", "2"),
                ImmutableList.of("us", "3"),
                ImmutableList.of("xyz", "2")
        );
        testIQL2(AllData.DATASET, expected,
                "FROM jobsearch yesterday today AS A ALIASING (country AS field)," +
                        " jobsearch yesterday today AS B ALIASING (ctkrcvd AS field) " +
                        "GROUP BY field",
                true
        );
    }

    @Test
    public void groupByAliasNotLastGroupBy() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
                ImmutableList.of("abc", "0", "0"),
                ImmutableList.of("abc", "1", "3"),
                ImmutableList.of("abd", "0", "0"),
                ImmutableList.of("abd", "1", "1"),
                ImmutableList.of("bcd", "0", "0"),
                ImmutableList.of("bcd", "1", "2"),
                ImmutableList.of("cn", "0", "0"),
                ImmutableList.of("cn", "1", "1"),
                ImmutableList.of("ddd", "0", "0"),
                ImmutableList.of("ddd", "1", "1"),
                ImmutableList.of("gb", "0", "0"),
                ImmutableList.of("gb", "1", "2"),
                ImmutableList.of("jp", "0", "0"),
                ImmutableList.of("jp", "1", "2"),
                ImmutableList.of("pqr", "0", "0"),
                ImmutableList.of("pqr", "1", "1"),
                ImmutableList.of("uk", "0", "0"),
                ImmutableList.of("uk", "1", "2"),
                ImmutableList.of("us", "0", "0"),
                ImmutableList.of("us", "1", "3"),
                ImmutableList.of("xyz", "0", "0"),
                ImmutableList.of("xyz", "1", "2")
        );
        testIQL2(AllData.DATASET, expected,
                "FROM jobsearch yesterday today AS A ALIASING (country AS field)," +
                        " jobsearch yesterday today AS B ALIASING (ctkrcvd AS field) " +
                        "GROUP BY field, (true)",
                true
        );
    }
}
