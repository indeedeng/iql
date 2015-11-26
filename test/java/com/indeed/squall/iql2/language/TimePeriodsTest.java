package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.util.core.Pair;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unchecked")
public class TimePeriodsTest {
    private static class TestDef {
        public final List<Pair<Integer, TimeUnit>> expected;
        public final String withSpaces;

        private TestDef(String withSpaces, Pair<Integer, TimeUnit>... expected) {
            this.expected = Arrays.asList(expected);
            this.withSpaces = withSpaces;
        }
    }
    
    private static List<TestDef> TEST_CASES = Arrays.asList(
            new TestDef("1 b", Pair.of(1, TimeUnit.BUCKETS)),
            new TestDef("1 bucket", Pair.of(1, TimeUnit.BUCKETS)),
            new TestDef("100 buckets", Pair.of(100, TimeUnit.BUCKETS)),
            new TestDef("1 y", Pair.of(1, TimeUnit.YEAR)),
            new TestDef("1 year", Pair.of(1, TimeUnit.YEAR)),
            new TestDef("100 years", Pair.of(100, TimeUnit.YEAR)),
            new TestDef("1 M", Pair.of(1, TimeUnit.MONTH)),
            new TestDef("1 month", Pair.of(1, TimeUnit.MONTH)),
            new TestDef("100 months", Pair.of(100, TimeUnit.MONTH)),
            new TestDef("1 w", Pair.of(1, TimeUnit.WEEK)),
            new TestDef("1 week", Pair.of(1, TimeUnit.WEEK)),
            new TestDef("100 weeks", Pair.of(100, TimeUnit.WEEK)),
            new TestDef("1 d", Pair.of(1, TimeUnit.DAY)),
            new TestDef("1 day", Pair.of(1, TimeUnit.DAY)),
            new TestDef("100 days", Pair.of(100, TimeUnit.DAY)),
            new TestDef("0h", Pair.of(0, TimeUnit.HOUR)),
            new TestDef("1 hour", Pair.of(1, TimeUnit.HOUR)),
            new TestDef("100 hours", Pair.of(100, TimeUnit.HOUR)),
            new TestDef("1 m", Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("1 minute", Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("100 minutes", Pair.of(100, TimeUnit.MINUTE)),
            new TestDef("1 s", Pair.of(1, TimeUnit.SECOND)),
            new TestDef("1 second", Pair.of(1, TimeUnit.SECOND)),
            new TestDef("100 seconds", Pair.of(100, TimeUnit.SECOND)),
            new TestDef("1234567890h", Pair.of(1234567890, TimeUnit.HOUR)),
            new TestDef(
                    "3d 5h 10m",
                    Pair.of(3, TimeUnit.DAY),
                    Pair.of(5, TimeUnit.HOUR),
                    Pair.of(10, TimeUnit.MINUTE)
            ),
            new TestDef(
                    "1s 15m 200h 50d 2w 1M 5y",
                    Pair.of(1, TimeUnit.SECOND),
                    Pair.of(15, TimeUnit.MINUTE),
                    Pair.of(200, TimeUnit.HOUR),
                    Pair.of(50, TimeUnit.DAY),
                    Pair.of(2, TimeUnit.WEEK),
                    Pair.of(1, TimeUnit.MONTH),
                    Pair.of(5, TimeUnit.YEAR)
            )
    );

    @Test
    public void testWithSpaces() throws Exception {
        for (final TestDef testCase : TEST_CASES) {
            checkOrderInvariant(testCase.expected, parseTimePeriod(testCase.withSpaces));
        }
    }

    @Test
    public void testWithoutSpaces() throws Exception {
        for (final TestDef testCase : TEST_CASES) {
            checkOrderInvariant(testCase.expected, parseTimePeriod(testCase.withSpaces.replace(" ", "")));
        }
    }

    private List<Pair<Integer, TimeUnit>> parseTimePeriod(String input) {
        final JQLParser.TimePeriodContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.TimePeriodContext>() {
            public JQLParser.TimePeriodContext apply(JQLParser input) {
                return input.timePeriodTerminal().timePeriod();
            }
        });
        return TimePeriods.parseTimePeriod(ctx);
    }

    private static <T> void checkOrderInvariant(List<T> expected, List<T> actual) {
        final Set<T> expectedSet = Sets.newHashSet(expected);
        Assert.assertEquals("Must have unique expected elements", expected.size(), expectedSet.size());
        final Set<T> actualSet = Sets.newHashSet(actual);
        Assert.assertEquals("Must have unique actual elements", actual.size(), actualSet.size());
        Assert.assertEquals(expectedSet, actualSet);
    }
}