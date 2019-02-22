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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldEqualFilterTest extends BasicTest {
    @Test
    public void testEqualFieldFilter() throws Exception {
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "3")), "from fieldEqual yesterday today where i1=i2");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "2")), "from fieldEqual yesterday today where s1=s2");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "2")), "from fieldEqual yesterday today where i1=i2 and s1=s2");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "2")), "from fieldEqual yesterday today, jobsearch(false) where fieldEqual.i1=fieldEqual.i2 and fieldEqual.s1=fieldEqual.s2");

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "2"));
        expected.add(ImmutableList.of("2", "1"));
        QueryServletTestUtils.testIQL2(expected, "from fieldEqual yesterday today where i1=i2 group by i1", true);

    }

    @Test
    public void testMultiDatasetEqualFieldFilter() {
        try {
            QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "fail")), "from fieldEqual yesterday today as o1, fieldEqual yesterday today as o2 where o1.i1 = o2.i2");
            Assert.fail("field on different dataset should throw exception");
        } catch (final Exception ignored) {
        }
    }

    @Test
    public void testNotEqualFieldFilter() throws Exception {
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "1")), "from fieldEqual yesterday today where i1!=i2");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "2")), "from fieldEqual yesterday today where s1!=s2");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "1")), "from fieldEqual yesterday today where i1!=i2 and s1!=s2");
    }
}
