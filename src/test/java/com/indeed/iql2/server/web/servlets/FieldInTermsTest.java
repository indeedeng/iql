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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldInTermsTest extends BasicTest {
    final Dataset dataset = AllData.DATASET;

    @Test
    public void testStringFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "145", "2"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk in ('a', \"b\") with default select count(), distinct(tk)");
        QueryServletTestUtils.testIQL2(dataset, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by tk in ('a', \"b\") with default select count()");
    }

    @Test
    public void testStringFieldNotInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "4", "1"));
        expected.add(ImmutableList.of("d", "141", "1"));
        expected.add(ImmutableList.of("DEFAULT", "6", "2"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk not in ('a', \"b\") with default select count(), distinct(tk)");
        QueryServletTestUtils.testIQL2(dataset, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by tk not in ('a', \"b\") with default select count()");
    }

    @Test
    public void testIntFieldInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "3"));
        expected.add(ImmutableList.of("10", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "65", "4"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by ojc in (1, 10) with default select count(), distinct(tk)", true);
        QueryServletTestUtils.testIQL2(dataset, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by ojc in (1, 10) with default select count()", true);
    }

    @Test
    public void testIntFieldNotInTerms() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("3", "60", "1"));
        expected.add(ImmutableList.of("5", "1", "1"));
        expected.add(ImmutableList.of("15", "1", "1"));
        expected.add(ImmutableList.of("DEFAULT", "86", "3"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by ojc not in (1, 10) with default select count(), distinct(tk)", true);
        QueryServletTestUtils.testIQL2(dataset, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by ojc not in (1, 10) with default select count()", true);
    }

    @Test
    public void testStreamingSubset() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testAll(dataset, expected, "from organic yesterday today group by tk in ('a', \"b\") select count()");
    }
}
