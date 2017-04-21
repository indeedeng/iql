package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExecutionWarningTest extends BasicTest {
    @Test
    public void testLimit() throws Exception {
        Assert.assertEquals(Collections.emptyList(),
                testLimitHelper("from organic yesterday today GROUP BY tk"));

        Assert.assertEquals(ImmutableList.of("Only first 1 rows returned sorted on the last group by column"),
                testLimitHelper("from organic yesterday today GROUP BY oji LIMIT 1"));

        Assert.assertEquals(ImmutableList.of("Only first 2 rows returned sorted on the last group by column"),
                testLimitHelper("from organic yesterday today GROUP BY tk LIMIT 2"));

        // limit equal to group by number
        Assert.assertEquals(Collections.emptyList(),
                testLimitHelper("from organic yesterday today GROUP BY tk LIMIT 4"));

        Assert.assertEquals(ImmutableList.of("Only first 5 rows returned sorted on the last group by column"),
                testLimitHelper("from organic yesterday today GROUP BY oji[10] LIMIT 5"));

        // limit equal to topk
        Assert.assertEquals(Collections.emptyList(),
                testLimitHelper("from organic yesterday today GROUP BY oji[5] LIMIT 5"));
    }

    private List<String> testLimitHelper(final String query) throws Exception {
        final JsonNode header = QueryServletTestUtils.getQueryHeader(OrganicDataset.create(), query,
                QueryServletTestUtils.LanguageVersion.IQL1, QueryServletTestUtils.Options.create());
        if (header.get("IQL-Warning") == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(header.get("IQL-Warning").textValue().split("\n"));
    }
}
