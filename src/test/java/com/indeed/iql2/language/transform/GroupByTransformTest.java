package com.indeed.iql2.language.transform;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.DocMetricsTest;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.indeed.iql2.language.transform.TransformTestUtil.findImplementationsOf;

public class GroupByTransformTest {
    private static List<Class> classesToTest;
    private static final List<Class> CLASSES_TO_SKIP = ImmutableList.of(
            GroupBy.GroupByFieldInQuery.class // requires much more test setup
    );

    @BeforeClass
    public static void findImplementations() throws IOException, ClassNotFoundException {
        classesToTest = findImplementationsOf(GroupBy.class);
        classesToTest.removeAll(CLASSES_TO_SKIP);
    }

    @AfterClass
    public static void ensureTested() {
        if (!classesToTest.isEmpty()) {
            final String untested = classesToTest.stream().map(Class::getSimpleName).collect(Collectors.joining(", "));
            throw new IllegalStateException("Failed to test classes: " + untested);
        }
    }

    private void test(final String text) {
        final GroupBy parsed = Queries.parseGroupBy(text, false, DocMetricsTest.CONTEXT);
        classesToTest.remove(parsed.getClass());
        Assert.assertEquals(text, parsed.getRawInput());
        final GroupBy transformed = parsed.transform(Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    @Test
    public void testGroupByField() {
        test("tk");
    }

    @Test
    public void testGroupByPredicate() {
        test("tk = 'a'");
    }

    @Test
    public void testGroupByMetric() {
        test("bucket(oji, 0, 11, 1)");
    }

    @Test
    public void testGroupByTime() {
        test("time(1d, 'yyyy-MM-dd', unixtime)");
    }

    @Test
    public void testGroupByTimeBucket() {
        test("time(1b, 'yyyy-MM-dd', unixtime)");
    }

    @Test
    public void testGroupByMonth() {
        test("time(1M, 'yyyy-MM-dd', unixtime)");
    }

    @Test
    public void GroupByInferredTime() {
        test("time()");
    }

    @Test
    public void testGroupByFieldIn() {
        test("oji in (1, 2, 3)");
    }

    @Test
    public void testGroupByDayOfWeek() {
        test("dayofweek()");
    }

    @Test
    public void testGroupBySessionName() {
        test("dataset()");
    }

    @Test
    public void testGroupByQuantiles() {
        test("quantiles(oji, 50)");
    }

    @Test
    public void testGroupByRandom() {
        test("random(unixtime, 5)");
    }

    @Test
    public void testGroupByRandomMetric() {
        test("random(hasint(oji, 1), 5)");
    }
}
