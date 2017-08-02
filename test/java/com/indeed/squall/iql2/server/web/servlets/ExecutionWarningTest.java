package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testWarning;

public class ExecutionWarningTest extends BasicTest {
    @Test
    public void testLimit() throws Exception {
        final Dataset dataset = OrganicDataset.create();
        testWarning(dataset, ImmutableList.of(), "from organic yesterday today GROUP BY tk");

        testWarning(dataset, ImmutableList.of("Only first 1 rows returned sorted on the last group by column"),
                "from organic yesterday today GROUP BY oji LIMIT 1");

        testWarning(dataset, ImmutableList.of("Only first 2 rows returned sorted on the last group by column"),
                "from organic yesterday today GROUP BY tk LIMIT 2");

        // limit equal to group by number
        testWarning(dataset, ImmutableList.of(),
                "from organic yesterday today GROUP BY tk LIMIT 4");

        testWarning(dataset, ImmutableList.of("Only first 5 rows returned sorted on the last group by column"),
                "from organic yesterday today GROUP BY oji[10] LIMIT 5");

        // limit equal to topk
        testWarning(dataset, ImmutableList.of(),
                "from organic yesterday today GROUP BY oji[5] LIMIT 5");
    }
}
