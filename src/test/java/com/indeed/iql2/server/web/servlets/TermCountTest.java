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
import com.indeed.iql2.server.web.servlets.dataset.MultiValuedDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TermCountTest extends BasicTest {
    @Test
    public void testIntTermCount() throws Exception {
        final Dataset dataset = AllData.DATASET;

        final List<List<String>> expected1 = new ArrayList<>();
        expected1.add(ImmutableList.of("", "33"));
        QueryServletTestUtils.testIQL2(dataset, expected1, "from termCount yesterday today where inttermcount(intField) = 1", true);

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "50"));
        QueryServletTestUtils.testIQL2(dataset, expected2, "from termCount yesterday today where inttermcount(intField) = 2", true);

        final List<List<String>> expected3 = new ArrayList<>();
        expected3.add(ImmutableList.of("", "17"));
        QueryServletTestUtils.testIQL2(dataset, expected3, "from termCount yesterday today where inttermcount(intField) = 3", true);
    }

    @Test
    public void testStrTermCount() throws Exception {
        final Dataset dataset = AllData.DATASET;

        final List<List<String>> expected1 = new ArrayList<>();
        expected1.add(ImmutableList.of("", "33"));
        QueryServletTestUtils.testIQL2(dataset, expected1, "from termCount yesterday today where strtermcount(strField) = 1", true);

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "50"));
        QueryServletTestUtils.testIQL2(dataset, expected2, "from termCount yesterday today where strtermcount(strField) = 2", true);

        final List<List<String>> expected3 = new ArrayList<>();
        expected3.add(ImmutableList.of("", "17"));
        QueryServletTestUtils.testIQL2(dataset, expected3, "from termCount yesterday today where strtermcount(strField) = 3", true);
    }

}
