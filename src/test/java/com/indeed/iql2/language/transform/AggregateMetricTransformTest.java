package com.indeed.iql2.language.transform;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetricsTest;
import com.indeed.iql2.language.query.Queries;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.indeed.iql2.language.transform.TransformTestUtil.findImplementationsOf;

public class AggregateMetricTransformTest {
    private static List<Class> classesToTest;
    private static final List<Class> CLASSES_TO_SKIP = ImmutableList.of(
            AggregateMetric.GroupStatsLookup.class, // Created via ExtractPrecomputed
            AggregateMetric.IterateLag.class, // Created for FTGS only
            AggregateMetric.NeedsSubstitution.class, // Created via aliases -- could be testable
            AggregateMetric.DocStatsPushes.class // Created via ExtractPrecomputed
    );

    @BeforeClass
    public static void findImplementations() throws IOException, ClassNotFoundException {
        classesToTest = findImplementationsOf(AggregateMetric.class);
        classesToTest.removeAll(CLASSES_TO_SKIP);
    }

    @AfterClass
    public static void ensureTested() {
        if (!classesToTest.isEmpty()) {
            final String untested = classesToTest.stream().map(Class::getSimpleName).collect(Collectors.joining(", "));
            throw new IllegalStateException("Failed to test classes: " + untested);
        }
    }

    private void test(final String metricText) {
        final AggregateMetric parsed = Queries.parseAggregateMetric(metricText, false, DocMetricsTest.CONTEXT);
        Assert.assertNotNull(parsed.getRawInput());
        classesToTest.remove(parsed.getClass());
        final AggregateMetric transformed = parsed.transform(Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    @Test
    public void testAdd() {
        test("oji + ojc");
    }

    @Test
    public void testLog() {
        test("log(oji)");
    }

    @Test
    public void testAbs() {
        test("abs(oji)");
    }

    @Test
    public void testSubstract() {
        test("oji - ojc");
    }

    @Test
    public void testMultiply() {
        test("oji * ojc");
    }

    @Test
    public void testDivide() {
        test("oji / ojc");
    }

    @Test
    public void testModulus() {
        test("oji % ojc");
    }

    @Test
    public void testPower() {
        test("oji ^ ojc");
    }

    @Test
    public void testParent() {
        test("parent(oji)");
    }

    @Test
    public void testLag() {
        test("lag(5, oji)");
    }

    @Test
    public void testWindow() {
        test("window(5, oji)");
    }

    @Test
    public void testQualified() {
        test("synthetic.oji");
    }

    @Test
    public void testConstant() {
        test("5");
    }

    @Test
    public void testPercentile() {
        test("percentile(oji, 95)");
    }

    @Test
    public void testRunning() {
        test("running(oji)");
    }

    @Test
    public void testDistinct() {
        test("distinct(oji)");
    }

    @Test
    public void testNamed() {
        test("distinct(oji) as doji");
    }

    @Test
    public void testSumAcross() {
        test("sum_over(oji, ojc)");
    }

    @Test
    public void testIfThenElse() {
        test("if oji > 0 then ojc else oji");
    }

    @Test
    public void testFieldMinAndFieldMax() {
        test("field_min(oji)");
        test("field_max(oji)");
    }


    @Test
    public void testMax() {
        test("max(oji, ojc)");
    }

    @Test
    public void testMin() {
        test("min(oji, ojc)");
    }

    @Test
    public void testDivideByCount() {
        test("avg(oji)");
    }

    @Test
    public void testDocStats() {
        test("[oji]");
    }

    @Test
    public void testNegate() {
        test("-oji");
    }
}
