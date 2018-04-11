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

package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MinMaxTest extends BasicTest {
    @Test
    public void testMinMaxBasics() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "0", "306", "306", "151", "2653", "2653"));
        QueryServletTestUtils.testIQL1(OrganicDataset.create(), expected, "from organic yesterday today select count(), min(0, 1), min(oji, ojc), ojc, max(0, 1), max(oji, ojc), oji");
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select count(), [min(0, 1)], [min(oji, ojc)], ojc, [max(0, 1)], [max(oji, ojc)], oji");
    }

    @Test
    public void testAggregateMinMax() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "0", "-10" ,"1", "7", "2653", "306"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select min(0,1), min(0,5,7,-10), max(0,1), max(0,5,7,-10), max(oji, ojc), min(oji, ojc)");
    }
}
