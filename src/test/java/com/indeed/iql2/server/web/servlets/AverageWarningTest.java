package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AverageWarningTest {
    private static List<String> getWarningMessage(final String avgStr, final List<String> operations) {
        return ImmutableList.of(
                "There are suspicious operations inside of " + avgStr + ". Are you sure you didn't mean AVG([...])? Operations were: "
                        + operations.stream().distinct().sorted().collect(Collectors.joining(","))
        );
    }

    private void testWarning(final List<String> operations, final String select) throws Exception {
        QueryServletTestUtils.testWarning(
                getWarningMessage(select, operations),
                "from organic yesterday today select " + select,
                QueryServletTestUtils.LanguageVersion.IQL2
        );
    }

    private void testNoWarning(final String select) throws Exception {
        QueryServletTestUtils.testWarning(
                Collections.emptyList(),
                "from organic yesterday today select " + select,
                QueryServletTestUtils.LanguageVersion.IQL2
        );
    }

    @Test
    public void testWarnings() throws Exception {
        testWarning(
                ImmutableList.of("division"),
                "avg(oji / ojc)"
        );
        testWarning(
                ImmutableList.of("multiplication"),
                "avg(oji * ojc)"
        );
        testWarning(
                ImmutableList.of("LOG"),
                "avg(LOG(oji))"
        );
        testWarning(
                ImmutableList.of("MIN", "MAX"),
                "avg(MIN(oji, ojc) + MAX(oji, ojc))"
        );
        testWarning(
                ImmutableList.of("modulo", "constant"),
                "avg(oji % 2)"
        );
        testWarning(
                ImmutableList.of("exponentiation", "constant"),
                "avg(oji ^ 2)"
        );
        testWarning(
                ImmutableList.of("FLOOR"),
                "avg(floor(oji))"
        );
        testWarning(
                ImmutableList.of("CEIL"),
                "avg(ceil(oji))"
        );
        testWarning(
                ImmutableList.of("ROUND"),
                "avg(round(oji))"
        );
        testWarning(
                ImmutableList.of("IF-THEN-ELSE"),
                "avg(if (true) then oji else ojc)"
        );
    }

    @Test
    public void testNoWarnings() throws Exception {
        testNoWarning("avg(oji + ojc - (-ojc))");
        testNoWarning("avg(ojc * 2 + 3 * oji)");
        testNoWarning("avg(distinct(oji))");
        testNoWarning("avg([oji] / [ojc])");
    }
}
