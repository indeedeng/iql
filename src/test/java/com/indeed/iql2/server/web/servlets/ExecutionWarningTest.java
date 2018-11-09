/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testWarning;

public class ExecutionWarningTest extends BasicTest {
    private final Dataset dataset = AllData.DATASET;

    @Test
    public void testLimit() throws Exception {
        final Dataset dataset = AllData.DATASET;
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

    @Test
    public void testMissingShards() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "182"));
        QueryServletTestUtils.testAll(dataset, expected, "from organic 2year today select count()");

        QueryServletTestUtils.testWarning(
                dataset,
                Lists.newArrayList("95% of the queried time period is missing in dataset organic: 2013-01-02/2014-12-01"),
                "from organic 2year today select count()");
    }
}
