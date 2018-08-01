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
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DateTimeTest extends BasicTest {
    @Test
    public void testWordDate() throws Exception {
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic y today select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "153", "2655", "308")), "from organic 3days ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic d ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60M ago select count(), oji, ojc");
        testIQL2(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 60M ago select count(), oji, ojc");
    }
}
