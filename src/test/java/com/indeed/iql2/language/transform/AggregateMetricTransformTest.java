package com.indeed.iql2.language.transform;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetricsTest;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.query.Queries;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
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

    private void testOnGroupedContext(final String metricText, final String groupByField) {
        final JQLParser.IdentifierContext groupByIdentifier = Queries.runParser(groupByField, JQLParser::identifierTerminal).identifier();
        final AggregateMetric parsed = Queries.parseAggregateMetric(metricText, false, DocMetricsTest.CONTEXT.withFieldAggregate(DocMetricsTest.CONTEXT.fieldResolver.resolve(groupByIdentifier)));
        classesToTest.remove(parsed.getClass());
        Assert.assertEquals(metricText, parsed.getRawInput());
        final AggregateMetric transformed = parsed.transform(Function.identity(), Function.identity(), Function.identity(), Function.identity(), Function.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    private void test(final String metricText) {
        final AggregateMetric parsed = Queries.parseAggregateMetric(metricText, false, DocMetricsTest.CONTEXT);
        classesToTest.remove(parsed.getClass());
        Assert.assertEquals(metricText, parsed.getRawInput());
        final AggregateMetric transformed = parsed.transform(Function.identity(), Function.identity(), Function.identity(), Function.identity(), Function.identity());
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
    public void testFloor() {
        test("floor(oji)");
        test("floor(oji, 2)");
    }

    @Test
    public void testCeil() {
        test("ceil(oji)");
        test("ceil(oji, 2)");
    }

    @Test
    public void testRound() {
        test("round(oji)");
        test("round(oji, 2)");
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
        testOnGroupedContext("parent(oji)", "ojc");
    }

    @Test
    public void testLag() {
        test("lag(5, oji)");
    }

    @Test
    public void testWindow() {
        testOnGroupedContext("window(5, oji)", "ojc");
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
        testOnGroupedContext("running(oji)", "ojc");
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
