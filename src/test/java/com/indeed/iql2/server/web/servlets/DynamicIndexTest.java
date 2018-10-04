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
import com.indeed.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

@Ignore("Test is ignored until ShardMaster supports dynamic indexes")
public class DynamicIndexTest extends BasicTest {
    @Test
    public void testUngroupedUsingDynamicIndex() throws Exception {
        final List<List<String>> expected = ImmutableList.<List<String>>of(ImmutableList.of("", "151", "2653", "306", "4"));
        QueryServletTestUtils.testAll(OrganicDataset.createWithDynamicShardNaming(), expected, "from organic yesterday today select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        QueryServletTestUtils.testAll(OrganicDataset.createWithDynamicShardNaming(), QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today select count(), oji, ojc");
    }
}
