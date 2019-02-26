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

public class SubQueryLimitTest extends BasicTest {
    @Test
    public void testQueryNoLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(expected, "from subQueryLimit yesterday today where f in (from same group by f) select count()", true);
    }

    @Test
    public void testQueryLimitNotTriggered() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(expected, "from subQueryLimit yesterday today where f in (from same group by f) select count()", QueryServletTestUtils.Options.create().setSkipTestDimension(true).setSubQueryTermLimit(200L));
        QueryServletTestUtils.testIQL2(expected, "from subQueryLimit yesterday today where f in (from same group by f) select count()", QueryServletTestUtils.Options.create().setSkipTestDimension(true).setSubQueryTermLimit(105L));
    }

    @Test
    public void testQueryLimitTriggered() throws Exception {
        QueryServletTestUtils.expectException(
                "from subQueryLimit yesterday today where f in (from same group by f) select count()",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create().setSkipTestDimension(true).setSubQueryTermLimit(104L),
                ex -> ex.contains("GroupLimitExceededException"));

        QueryServletTestUtils.expectException(
                "from subQueryLimit yesterday today where f in (from same group by f) select count()",
                QueryServletTestUtils.LanguageVersion.IQL2,
                QueryServletTestUtils.Options.create().setSkipTestDimension(true).setSubQueryTermLimit(1L),
                ex -> ex.contains("GroupLimitExceededException"));
    }
}
