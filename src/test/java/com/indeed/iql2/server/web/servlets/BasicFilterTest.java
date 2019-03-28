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

public class BasicFilterTest extends BasicTest {
    @Test
    public void testBasicFilters() throws Exception {
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"a\" select count()");
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("", "2")), "from organic yesterday today where tk=\"b\" select count()");
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"c\" select count()");
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("", "141")), "from organic yesterday today where tk=\"d\" select count()");
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "141")), "from organic yesterday today where tk:d select count()");
    }

    @Test
    public void testMultipleDatasetFilters() throws Exception {
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "2")), "from dataset1 yesterday today, dataset2 where intField1=1 select count()");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "22")), "from dataset1 1month today, dataset2 where strField1='1' select count()");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "3")), "from dataset1 1w today, dataset2 where dataset1.intField2=1 select dataset1.count()");
    }

    @Test
    public void testNegateFilters() throws Exception {
        // Iql1 does not support unary NOT
        QueryServletTestUtils.testIQL2AndLegacy(ImmutableList.of(ImmutableList.of("", "149")), "from organic yesterday today where NOT(tk=\"b\") select count()");
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "147")), "from organic yesterday today where tk!=\"a\" select count()");
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "147")), "from organic yesterday today where -tk:c select count()");
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "10")), "from organic yesterday today where -tk=~'d' select count()");
    }

    @Test
    public void testBetweenFilters() throws Exception {
        // 'between(...)' is different in Iql1 (upper bound is included) and Iql2 (upper is excluded), so two tests here.
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "134")), "from organic yesterday today where between(oji, 5, 10) SELECT counts");
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "134")), "from organic yesterday today where between(oji + oji, 10, 20) SELECT counts");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "5")), "from organic yesterday today where between(oji, 5, 10) SELECT counts");
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "5")), "from organic yesterday today where between(oji + oji, 10, 20) SELECT counts");
    }

    @Test
    public void testHasStrOnIntField() throws Exception {
        // Test that in Iql1 hasstr accepts any field.
        // field = "str" and field != "str" lead to 'hasstr(field,"str")' pushStat so test it here as well
        QueryServletTestUtils.testIQL1(ImmutableList.of(ImmutableList.of("", "129", "0", "151", "0", "0")),
                "from organic yesterday today select hasint(oji, 10), hasstr(oji, \"10\"), oji != \"sldgkslkkj\", oji = \"lskdgj\", hasstr(\"oji:lskjg\")", true);
    }
}
