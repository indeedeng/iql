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

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;

public class PrintfTest extends BasicTest {
    @Test
    public void testGetGroupStats() throws Exception {
        // More convoluted than printf(1/3, '%0.2d') to support both IQL1 and IQL2.
        // In IQL1, 1/3 == count() / 3
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "0.33"));
        expected.add(ImmutableList.of("b", "0.33"));
        expected.add(ImmutableList.of("c", "0.33"));
        expected.add(ImmutableList.of("d", "0.33"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by tk select printf('%.2f', count()/(3*count()))", true);
    }

    @Test
    public void testWithGroupBy() throws Exception {
        // More convoluted than printf(1/3, '%0.2d') to support both IQL1 and IQL2.
        // In IQL1, 1/3 == count() / 3
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "0.33")), "from organic yesterday today select printf('%.2f', count()/(3*count()))");
    }
}
