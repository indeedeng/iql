package com.indeed.iql2.language.transform;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.AggregateFilter;
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

public class AggregateFilterTransformTest {
    private static List<Class> classesToTest;
    private static final List<Class> CLASSES_TO_SKIP = ImmutableList.of(
            AggregateFilter.IsDefaultGroup.class // Is used for making group bys total, cannot be typed right now
    );

    @BeforeClass
    public static void findImplementations() throws IOException, ClassNotFoundException {
        classesToTest = findImplementationsOf(AggregateFilter.class);
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
        final AggregateFilter parsed = Queries.parseAggregateFilter(metricText, false, DocMetricsTest.CONTEXT);
        Assert.assertNotNull(parsed.getRawInput());
        classesToTest.remove(parsed.getClass());
        final AggregateFilter transformed = parsed.transform(Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    @Test
    public void testTermIs() {
        test("term() = 'a'");
    }

    @Test
    public void testTermRegex() {
        test("term() =~ 'a*'");
    }

    @Test
    public void testMetricIs() {
        test("ojc = 1");
    }

    @Test
    public void testMetricIsnt() {
        test("ojc != 1");
    }

    @Test
    public void testGt() {
        test("ojc > 1");
    }

    @Test
    public void testGte() {
        test("ojc >= 1");
    }

    @Test
    public void testLt() {
        test("ojc < 1");
    }

    @Test
    public void testLte() {
        test("ojc <= 1");
    }

    @Test
    public void testAnd() {
        test("ojc <= 1 and oji >=1");
    }

    @Test
    public void testOr() {
        test("ojc <= 1 or oji >=1");
    }

    @Test
    public void testNot() {
        test("!ojc = 1");
    }

    @Test
    public void testRegex() {
        test("tk =~ 'a*'");
    }

    @Test
    public void testAlways() {
        test("true");
    }

    @Test
    public void testNever() {
        test("false");
    }
}
