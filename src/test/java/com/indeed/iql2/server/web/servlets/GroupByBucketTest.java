package com.indeed.iql2.server.web.servlets;

import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class GroupByBucketTest {

    @Test
    public void invalidBucketSize() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        try {
            testIQL2(AllData.DATASET, expected, "FROM organic yesterday today GROUP BY bucket(oji,1,95,10) SELECT count()");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("com.indeed.iql.exceptions.IqlKnownException$ParseErrorException"));
        }

        try {
            testIQL2(AllData.DATASET, expected, "FROM organic yesterday today GROUP BY bucket(oji,1,99,10) SELECT count()");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("com.indeed.iql.exceptions.IqlKnownException$ParseErrorException"));
        }

    }

}
