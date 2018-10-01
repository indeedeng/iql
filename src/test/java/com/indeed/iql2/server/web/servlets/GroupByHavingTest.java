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
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.dataset.GroupByHavingDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class GroupByHavingTest extends BasicTest {
    @Test
    public void testHaving() throws Exception {
        testIQL2(AllData.DATASET, ImmutableList.<List<String>>of(ImmutableList.of("0", "3")), "FROM test yesterday today GROUP BY label HAVING val > 2", true);
    }

    @Test
    public void testHavingDivide() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 12:00:00)", "0.5"));
        testIQL2(AllData.DATASET, expected, "FROM test yesterday today " +
                "GROUP BY time(12h) HAVING label=1 / count() > 0 SELECT label=1 / count()");
    }
}
