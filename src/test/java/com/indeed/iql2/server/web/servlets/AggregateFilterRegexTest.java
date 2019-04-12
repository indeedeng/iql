package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql.testutil.ExceptionMatcher;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for deprecated feature {@link com.indeed.iql2.language.AggregateFilter.Regex}.
 */
public class AggregateFilterRegexTest {
    @Test
    public void testGroupByHaving() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk having tk=~'[a-c]' select count()", true);
        expected.add(ImmutableList.of("DEFAULT", "141"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk with default having tk=~'[a-c]' select count()", true);
    }

    @Test
    public void testMultipleGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "a", "1"));
        expected.add(ImmutableList.of("1", "a", "2"));
        expected.add(ImmutableList.of("5", "a", "1"));
        expected.add(ImmutableList.of("2", "b", "1"));
        expected.add(ImmutableList.of("15", "b", "1"));
        expected.add(ImmutableList.of("0", "c", "1"));
        expected.add(ImmutableList.of("1", "c", "1"));
        expected.add(ImmutableList.of("10", "c", "2"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc, tk having tk=~'[a-c]' select count()", true);
    }

    @Test
    public void testMultipleGroupByNotLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "0", "1"));
        expected.add(ImmutableList.of("c", "0", "1"));
        expected.add(ImmutableList.of("a", "1", "2"));
        expected.add(ImmutableList.of("c", "1", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("a", "5", "1"));
        expected.add(ImmutableList.of("c", "10", "2"));
        expected.add(ImmutableList.of("b", "15", "1"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk having tk=~'[a-c]', ojc select count()", true);
    }

    @Test
    @Ignore // Until IQL-905
    public void testParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "0", "1"));
        expected.add(ImmutableList.of("c", "0", "1"));
        expected.add(ImmutableList.of("a", "1", "2"));
        expected.add(ImmutableList.of("c", "1", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("a", "5", "1"));
        expected.add(ImmutableList.of("c", "10", "2"));
        expected.add(ImmutableList.of("b", "15", "1"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk, oji having parent(M(tk =~'[a-c]'))=1 select count()", true);
    }

    @Test
    public void testIfThenElse() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "0"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk select if tk =~ '[a-c]' then [1] else [0]", true);
    }

    @Test
    public void testM() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1"));
        expected.add(ImmutableList.of("b", "1"));
        expected.add(ImmutableList.of("c", "1"));
        expected.add(ImmutableList.of("d", "0"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk select M(tk =~ '[a-c]')", true);
    }

    @Test
    public void testDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "2"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "0"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "1"));
        expected.add(ImmutableList.of("15", "1"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc select distinct(tk having tk =~ '[a-c]')", true);
    }

    @Test
    public void testDistinctWindow() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "2"));
        expected.add(ImmutableList.of("2", "3"));
        expected.add(ImmutableList.of("3", "1"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "2"));
        expected.add(ImmutableList.of("15", "2"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc select distinct_window(2, tk having tk =~ '[a-c]')", true);
    }

    @Test
    public void testSumOver() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "2"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "0"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "1"));
        expected.add(ImmutableList.of("15", "1"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc select sum_over(tk having tk =~ '[a-c]', 1)", true);
    }

    @Test
    public void testAvgOver() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "1"));
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "NaN"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "1"));
        expected.add(ImmutableList.of("15", "1"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc select avg_over(tk having tk =~ '[a-c]', 1)", true);
    }

    @Test
    @Ignore // until IQL-906
    public void testFieldMin() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "0"));
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "2"));
        expected.add(ImmutableList.of("3", "3"));
        expected.add(ImmutableList.of("5", "NaN"));
        expected.add(ImmutableList.of("10", "NaN"));
        expected.add(ImmutableList.of("15", "NaN"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by ojc select field_min(ojc having ojc =~ '[0-3]')", true);
    }

    @Test
    public void testWarnings() throws Exception {
        final List<String> expected = ImmutableList.of("Aggregate regex (e.g. HAVING fieldName =~ \"regex\") is deprecated. Please use TERM() =~ \"regex\" or NOT(TERM() =~ \"regex\") instead.");
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today group by tk having tk =~ '.*'",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today group by tk select if tk =~ '.*' then [1] else [0]",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today group by tk select M(tk =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today select distinct(tk having tk =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today select distinct_window(2, tk having tk =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today select sum_over(tk having tk =~ '.*', 1)",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                expected,
                "from organic yesterday today select avg_over(tk having tk =~ '.*', 1)",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
    }

    @Test
    public void testWrongUsages() {
        final ExceptionMatcher<IllegalArgumentException> exceptionMatcher = ExceptionMatcher
                .withType(IllegalArgumentException.class)
                .withMessage(Matchers.containsString("Did you mean [oji =~ \"...\"] > 0?"));

        QueryServletTestUtils.expectException(
                "from organic yesterday today group by tk having oji =~ '.*'",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today group by tk select if oji =~ '.*' then [1] else [0]",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today group by tk select M(oji =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today select distinct(tk having oji =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today select distinct_window(2, tk having oji =~ '.*')",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today select sum_over(tk having oji =~ '.*', 1)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today select avg_over(tk having oji =~ '.*', 1)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create(),
                exceptionMatcher
        );
    }
}
