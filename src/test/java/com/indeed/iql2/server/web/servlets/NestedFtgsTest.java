package com.indeed.iql2.server.web.servlets;

import com.google.common.base.Predicates;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion.IQL2;

public class NestedFtgsTest extends BasicTest {
    @Test
    public void directNesting() {
        final String query =
                "FROM organic 10d today " +
                "GROUP BY country[top 10 by PERCENTILE(oji, 95)]";
        QueryServletTestUtils.expectException(query, IQL2, x -> x.contains("Cannot use value that has not been computed yet"));
    }

    @Test
    public void indirectNesting() {
        final String query =
                "from organic 2d 1d " +
                "group by country[top 10 by distinct_tk]" +
                "select DISTINCT(tk) as distinct_tk";
        QueryServletTestUtils.expectException(query, IQL2, x -> x.contains("Cannot use value that has not been computed yet"));
    }

    @Test
    @Ignore("Test after supporting deeper dependent computations")
    public void sameDepth() {
        final String query =
                "from organic 2d 1d " +
                "select DISTINCT(country) as c, DISTINCT(oji HAVING count() > c)";
        QueryServletTestUtils.expectException(query, IQL2, Predicates.alwaysTrue());
    }
}
