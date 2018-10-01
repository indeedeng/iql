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
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class IQL339Test extends BasicTest {
    @Test
    public void testBasicFilters() throws Exception {
        final Dataset dataset = AllData.DATASET;
        QueryServletTestUtils.testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji", true);
        QueryServletTestUtils.testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji", true);
        QueryServletTestUtils.testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji, oji", true);
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(), "from organic(oji=-1) yesterday today group by oji, oji, oji", true);
    }
}
