package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.Options;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.runIQL2;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jwolfe
 */
public class IdentifierResolutionTest extends BasicTest {
    @Test
    public void testUnknownDatasetRejected() {
        try {
            runIQL2("from foobarbaz123 yesterday today");
            fail("Unknown dataset not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownDatasetException"));
        }
    }

    @Test
    public void testUnknownFieldRejected() throws Exception {
        // Just ensure organic exists, as setup for the test to have meaning.
        runIQL2("from organic yesterday today");

        try {
            runIQL2("from organic yesterday today select myImaginaryField");
            fail("Unknown field not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownFieldException"));
        }
    }

    @Test
    public void metricAsFieldRejected() throws Exception {
        // Just to ensure metric aliases work, as a setup for the test to have meaning.
        runIQL2("from organic yesterday today select oji + 2 as foo, foo + 5");

        try {
            runIQL2("from organic yesterday today select oji + 2 as foo, distinct(foo)");
            fail("Metric used in FTGS field not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("Metric alias cannot be used as a field"));
        }
    }

    @Test
    public void dimensionsMetricAsFieldRejected() {
        final ImhotepClient client = AllData.DATASET.getDimensionsClient();

        try {
            runIQL2(client, "from dimension yesterday today select distinct(i1divi2)", Options.create(true).setImsClient(AllData.DATASET.getDimensionImsClient()));
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownFieldException"));
        }
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
        try {
            runIQL2("from EXACTCASE yesterday today");
            fail("Ambiguous dataset not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownDatasetException"));
        }

        try {
            runIQL2("from organic yesterday today as oOo1, organic as OOO1 select ooo1.count()");
            fail("Ambiguous dataset not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownDatasetException"));
        }

        try {
            runIQL2("from organic yesterday today as oOo1, organic as OOO1 select ooo1.count()");
            fail("Ambiguous dataset not rejected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("UnknownDatasetException"));
        }
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
