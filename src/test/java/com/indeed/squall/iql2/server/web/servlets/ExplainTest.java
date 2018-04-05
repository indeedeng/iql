package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.runAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.runIQL2;

public class ExplainTest extends BasicTest {
    @Test
    public void testExplain() throws Exception {
        runAll(OrganicDataset.create().getShards(), "explain from organic yesterday today where tk=\"a\" select count()");
        runAll(OrganicDataset.create().getShards(), "explain from organic yesterday today group by tk[5], oji, ojc select count()");
    }

    @Test
    public void testSubqueryExplain() throws Exception {
        runIQL2(OrganicDataset.create().getShards(), "explain from organic yesterday today as o1 where o1.tk in (from same group by tk) and oji in (from same where oji=1 group by ojc) group by tk select count()");
    }
}
