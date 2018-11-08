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
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IQL704Test extends BasicTest {
    @Test
    public void ensureErrorThrown() throws Exception {
        final Dataset dataset = AllData.DATASET;
        try {
            QueryServletTestUtils.testIQL1(dataset, ImmutableList.of(), "from big yesterday today group by field, field", true);
            Assert.fail();
        } catch (Exception e) {
            // Ensure we get *just* above the limit, instead of way above
            Assert.assertTrue(e.getMessage().contains("Number of groups [1000001] exceeds the group limit [1000000]"));
        }
        try {
            QueryServletTestUtils.testIQL2(dataset, ImmutableList.of(), "from big yesterday today group by field, field", true);
            Assert.fail();
        } catch (Exception e) {
            // Ensure we get *just* above the limit, instead of way above
            Assert.assertTrue(e.getMessage().contains("Number of groups [1000001] exceeds the group limit [1000000]"));
        }
    }
}
