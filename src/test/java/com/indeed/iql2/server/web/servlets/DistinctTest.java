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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DistinctTest extends BasicTest {

    @Test
    public void basicDistinct() throws Exception {
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "4")), "from distinct yesterday 2015-01-10 select distinct(tk)");
    }

    @Test
    public void timeDistinct() throws Exception {
        testAll(AllData.DATASET, ImmutableList.of(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "3"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "2"),
                ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "0"),
                ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0")
        ), "from distinct yesterday 2015-01-10 group by time(1d) select distinct(tk)");
    }

    @Test
    public void timeDistinctWindow() throws Exception {
        testIQL2(AllData.DATASET, ImmutableList.of(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "4"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "3"),
                ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "2"),
                ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0")
        ), "from distinct yesterday 2015-01-10 group by time(1d) select distinct_window(2, tk)");
    }

    @Test
    public void timeDistinctWindow2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"));
        for (int i = 3; i < 30; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-%02d 00:00:00, 2015-01-%02d 00:00:00)", i, i + 1), "4"));
        }
        testIQL2(AllData.DATASET, expected, "from distinct yesterday 2015-01-30 group by time(1d) select distinct_window(30, tk)");
    }
}
