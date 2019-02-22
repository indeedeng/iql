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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PredicateRegroupTest extends BasicTest {
    @Test
    public void singleDataset() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "149"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by oji < 100 select count()");
        QueryServletTestUtils.testIQL2(QueryServletTestUtils.addConstantColumn(1, "1", expected),
                "from organic yesterday today group by oji < 100, allbit select count()", true);
    }

    @Test
    public void dualDataset() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "149"));
        QueryServletTestUtils.testIQL2(expected, "from organic 24h 12h as o1, organic 12h 0h as o2 group by oji < 100 select count()");
        QueryServletTestUtils.testIQL2(QueryServletTestUtils.addConstantColumn(1, "1", expected),
                "from organic 24h 12h as o1, organic 12h 0h as o2  group by oji < 100, allbit select count()", true);
    }
}
