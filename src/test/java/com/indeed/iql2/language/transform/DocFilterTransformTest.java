package com.indeed.iql2.language.transform;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.DocFilter;
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

public class DocFilterTransformTest {
    private static List<Class> classesToTest;
    private static final List<Class> CLASSES_TO_SKIP = ImmutableList.of(
            DocFilter.ExplainFieldIn.class, // Used internally for EXPLAIN
            DocFilter.Qualified.class // Used internally for FROM clause filters
    );

    @BeforeClass
    public static void findImplementations() throws IOException, ClassNotFoundException {
        classesToTest = findImplementationsOf(DocFilter.class);
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
        final DocFilter parsed = Queries.parseDocFilter(metricText, false, DocMetricsTest.CONTEXT);
        Assert.assertNotNull(parsed.getRawInput());
        classesToTest.remove(parsed.getClass());
        final DocFilter transformed = parsed.transform(Functions.identity(), Functions.identity());
        Assert.assertEquals(parsed, transformed);
        Assert.assertNotSame(parsed, transformed);
        Assert.assertEquals(parsed.getRawInput(), transformed.getRawInput());
    }

    @Test
    public void testFieldIs() {
        test("tk=\"a\" ");
    }

    @Test
    public void testFieldIsnt() {
        test("tk!=\"a\" ");
    }

    @Test
    public void testMetricEqual() {
        test("ABS(ojc) = ABS(oji) ");
    }

    @Test
    public void testFieldInQuery() {
        test("tk not in (from jobsearch yesterday today group by ctkrcvd)");
    }

    @Test
    public void testBetween() {
        test("between(unixtime, 0, 1000)");
    }

    @Test
    public void testMetricNotEqual() {
        test("EXP(ojc) != EXP(oji)");
    }

    @Test
    public void testMetricGt() {
        test("EXP(ojc) > EXP(oji)");
    }

    @Test
    public void testMetricGte() {
        test("EXP(ojc) >= EXP(oji)");
    }

    @Test
    public void testMetricLt() {
        test("EXP(ojc) < EXP(oji)");
    }

    @Test
    public void testMetricLte() {
        test("EXP(ojc) <= EXP(oji)");
    }

    @Test
    public void testAnd() {
        test("EXP(ojc) <= EXP(oji) and between(unixtime, 0, 1000)");
    }

    @Test
    public void testOr() {
        test("EXP(ojc) <= EXP(oji) or between(unixtime, 0, 1000)");
    }

    @Test
    public void testNot() {
        test("not(tk=\"a\") ");
    }

    @Test
    public void testRegex() {
        test("tk =~ \"a.*\" ");
    }

    @Test
    public void testNotRegex() {
        test("tk !=~ \"a.*\" ");
    }

    @Test
    public void testQualified() {
        test("synthetic.tk = \"a\"");
    }

    @Test
    public void testLucene() {
        test("lucene(\"oji:3\")");
    }

    @Test
    public void testSample() {
        test("sample(unixtime, 1)");
    }

    @Test
    public void testAlways() {
        test("true");
    }

    @Test
    public void testNever() {
        test("false");
    }

    @Test
    public void testStringFieldIn() {
        test("tk in ('a', 'b')");
    }

    @Test
    public void testIntFieldIn() {
        test("ojc in (1, 2)");
    }

    @Test
    public void testFieldEqual() {
        test("ojc = oji");
    }

    @Test
    public void sampleDocMetric() {
        test("sample(oji + 10, 50)");
    }
}
