package com.indeed.iql2.language.transform;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetricsTest;
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

public class DocMetricTransformTest {
    private static List<Class> classesToTest;
    private static final List<Class> CLASSES_TO_SKIP = ImmutableList.of(
            DocMetric.PerDatasetDocMetric.class, // Not writeable
            DocMetric.Qualified.class, // Why?
            DocMetric.Sample.class, // Cannot be written without m(), so hard to use in this simple test
            DocMetric.SampleMetric.class, // Cannot be written without m(), so hard to use in this simple test
            DocMetric.FieldInQueryPlaceholderMetric.class // This is just a place holder and not writable
    );

    @BeforeClass
    public static void findImplementations() throws IOException, ClassNotFoundException {
        classesToTest = findImplementationsOf(DocMetric.class);
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
        final DocMetric parsed = Queries.parseDocMetric(metricText, false, DocMetricsTest.CONTEXT);
        classesToTest.remove(parsed.getClass());
        Assert.assertEquals(metricText, parsed.getRawInput());
        final DocMetric transformed = parsed.transform(Function.identity(), Function.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    @Test
    public void testLog() {
        test("log(oji)");
    }

    @Test
    public void testCount() {
        test("count()");
    }

    @Test
    public void testDocId() {
        test("docid()");
    }

    @Test
    public void testDocField() {
        test("tk");
    }

    @Test
    public void testExponentiate() {
        test("EXP(oji)");
    }

    @Test
    public void testNegate() {
        test("-EXP(oji)");
    }

    @Test
    public void testAbs() {
        test("ABS(oji)");
    }

    @Test
    public void testSignum() {
        test("SIGNUM(oji)");
    }

    @Test
    public void testAdd() {
        test("oji + ojc");
    }

    @Test
    public void testSubtract() {
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
    public void testMin() {
        test("min(oji, ojc)");
    }

    @Test
    public void testMax() {
        test("max(oji, ojc)");
    }

    @Test
    public void testMetricEqualAndMetricNotEqual() {
        test("abs(oji) = abs(ojc)");
        test("signum(oji) != signum(ojc)");
    }

    @Test
    public void testMetricLtAndMetricGt() {
        test("abs(oji) < abs(ojc)");
        test("signum(oji) > signum(ojc)");
    }

    @Test
    public void testMetricLteAndMetricGte() {
        test("abs(oji) <= abs(ojc)");
        test("signum(oji) >= signum(ojc)");
    }

    @Test
    public void testExtract() {
        test("extract(tk, '.*')");
    }

    @Test
    public void testRegex() {
        test("tk =~ '.*'");
    }

    @Test
    public void testFloatscale() {
        test("floatscale(ojc, 1, 2)");
    }

    @Test
    public void testConstant() {
        test("5");
    }

    @Test
    public void testHasIntField() {
        test("hasintfield(oji)");
    }

    @Test
    public void testHasStringField() {
        test("hasstrfield(tk)");
    }

    @Test
    public void testIntTermCount() {
        test("inttermcount(oji)");
    }

    @Test
    public void testStringTermCount() {
        test("strtermcount(tk)");
    }

    @Test
    public void testHasInt() {
        test("hasint(oji, 5)");
    }

    @Test
    public void testHasString() {
        test("hasstr(tk, 'a')");
    }

    @Test
    public void testIfThenElse() {
        test("if(tk = 'a') then 1 else 0");
    }

    @Test
    public void testQualified() {
        test("synthetic.oji");
    }

    @Test
    public void testFieldEqual() {
        test("oji = ojc");
    }

    @Test
    public void testStringLength() {
        test("len(tk)");
    }

    @Test
    public void testLucene() {
        test("lucene(\"oji:10\")");
    }

    @Test
    public void testRandom() {
        test("random(oji, 30)");
    }

    @Test
    public void testRandomMetric() {
        test("random(oji+10, 30)");
    }
}
