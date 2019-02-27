package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.Options;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.expectException;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.expectExceptionAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.runIQL2;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

/**
 * @author jwolfe
 */
public class IdentifierResolutionTest extends BasicTest {
    @Test
    public void testUnknownDatasetRejected() {
        expectExceptionAll("from foobarbaz123 yesterday today", ex -> ex.contains("UnknownDatasetException"));
    }

    @Test
    public void testUnknownFieldRejected() throws Exception {
        // Just ensure organic exists, as setup for the test to have meaning.
        runIQL2("from organic yesterday today");

        expectExceptionAll("from organic yesterday today select myImaginaryField", ex -> ex.contains("UnknownFieldException"));
    }

    @Test
    public void metricAsFieldRejected() throws Exception {
        // Just to ensure metric aliases work, as a setup for the test to have meaning.
        runIQL2("from organic yesterday today select oji + 2 as foo, foo + 5");

        expectException("from organic yesterday today select oji + 2 as foo, distinct(foo)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                ex -> ex.contains("Metric alias cannot be used as a field"));
    }

    @Test
    public void dimensionsMetricAsFieldRejected() {
        expectExceptionAll("from DIMension yesterday today select distinct(i1divi2)",
                Options.create(true).setImsClient(AllData.DATASET.getDimensionImsClient()),
                ex -> ex.contains("UnknownFieldException"));
    }

    @Test
    public void testCaseInsensitiveDatasetResolution() throws Exception {
        testIQL2(
                ImmutableList.of(ImmutableList.of("", "151")),
                "from ORGaNiC yesterday today"
        );
    }

    @Test
    public void testCaseInsensitiveFieldAliasResolution() throws Exception {
        testIQL2(
                ImmutableList.of(ImmutableList.of("", "2653")),
                "from organic yesterday today aliasing (oJi as foo) select FOO",
                true
        );
    }

    @Test
    public void testCaseInsensitiveMetricAliasResolution() throws Exception {
        testIQL2(
                ImmutableList.of(ImmutableList.of("", "2656", "2656")),
                "from organic yesterday today select oJi+3 as foo, FOO",
                true
        );
    }

    @Test
    public void testCaseInsensitiveFieldResolution() throws Exception {
        testIQL2(
                ImmutableList.of(ImmutableList.of("", "2653")),
                "from organic yesterday today select OJi",
                true
        );
    }

    @Test
    public void testAmbiguousDatasetRejected() {
        expectExceptionAll("from EXACTCASE yesterday today", ex -> ex.contains("UnknownDatasetException"));

        expectException("from organic yesterday today as oOo1, organic as OOO1 select ooo1.count()",
                QueryServletTestUtils.LanguageVersion.IQL2,
                ex -> ex.contains("UnknownDatasetException"));

        expectException("from organic yesterday today as oOo1, organic as OOO1 select ooo1.count()",
                QueryServletTestUtils.LanguageVersion.IQL2,
                ex -> ex.contains("UnknownDatasetException"));
    }

    @Test
    public void testExactCaseDatasetMatching() throws Exception {
        testIQL2(
                ImmutableList.of(ImmutableList.of("", "1", "100")),
                "from ExactCase yesterday today as dataset1, exactCase as DATASET1 select dataset1.i1, dataset1.I1"
        );
    }

    @Test
    public void testExactCaseMatching() throws Exception {
        testAll(
                ImmutableList.of(ImmutableList.of("", "1", "100")),
                "from ExactCase yesterday today select i1, I1"
        );
        testAll(
                ImmutableList.of(ImmutableList.of("1", "100", "1")),
                "from ExactCase yesterday today group by i1, I1",
                true
        );
        testAll(
                ImmutableList.of(ImmutableList.of("", "1", "1")),
                "from ExactCase yesterday today select distinct(i1), distinct(I1)",
                true
        );

        testAll(
                ImmutableList.of(ImmutableList.of("100", "One Hundred", "1")),
                "from ExactCase yesterday today group by f1, F1",
                true
        );
        testAll(
                ImmutableList.of(ImmutableList.of("", "1", "1")),
                "from ExactCase yesterday today select distinct(f1), distinct(F1)",
                true
        );


        testAll(
                ImmutableList.of(ImmutableList.of("s1 term", "S1 TERM", "1")),
                "from exactCase yesterday today group by s1, S1",
                true
        );
        testAll(
                ImmutableList.of(ImmutableList.of("", "1", "1")),
                "from exactCase yesterday today select distinct(s1), distinct(S1)",
                true
        );
    }
}
