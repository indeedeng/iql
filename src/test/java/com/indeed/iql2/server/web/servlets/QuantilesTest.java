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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QuantilesTest extends BasicTest {
    @Test
    public void test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 0.2)", "20", "0", "9.5", "19"));
        expected.add(ImmutableList.of("[0.2, 0.4)", "20", "20", "29.5", "39"));
        expected.add(ImmutableList.of("[0.4, 0.6)", "20", "40", "49.5", "59"));
        expected.add(ImmutableList.of("[0.6, 0.8)", "20", "60", "69.5", "79"));
        expected.add(ImmutableList.of("[0.8, 1.0)", "20", "80", "89.5", "99"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_mandatory yesterday today group by quantiles(f, 5) select count(), field_min(f), f / count(), field_max(f)", true);
    }

    @Test
    public void testMultiSession() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 0.2)", "40", "0", "9.5", "19"));
        expected.add(ImmutableList.of("[0.2, 0.4)", "40", "20", "29.5", "39"));
        expected.add(ImmutableList.of("[0.4, 0.6)", "40", "40", "49.5", "59"));
        expected.add(ImmutableList.of("[0.6, 0.8)", "40", "60", "69.5", "79"));
        expected.add(ImmutableList.of("[0.8, 1.0)", "40", "80", "89.5", "99"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_mandatory yesterday today as A, quantiles_mandatory as B group by quantiles(f, 5) select count(), field_min(f), f / count(), field_max(f)", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultiValue() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_multi_value yesterday today group by quantiles(f, 5) select field_min(f), f / count(), field_max(f)", true);
    }

    @Test
    public void testOptionalField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 0.2)", "10", "0", "9", "18"));
        expected.add(ImmutableList.of("[0.2, 0.4)", "10", "20", "29", "38"));
        expected.add(ImmutableList.of("[0.4, 0.6)", "10", "40", "49", "58"));
        expected.add(ImmutableList.of("[0.6, 0.8)", "10", "60", "69", "78"));
        expected.add(ImmutableList.of("[0.8, 1.0)", "10", "80", "89", "98"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_optional yesterday today group by quantiles(f, 5) select count(), field_min(f), f / count(), field_max(f)", true);
    }

    @Test
    public void testOptionalFieldWithMultiSession() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 0.2)", "20", "0", "9", "18"));
        expected.add(ImmutableList.of("[0.2, 0.4)", "20", "20", "29", "38"));
        expected.add(ImmutableList.of("[0.4, 0.6)", "20", "40", "49", "58"));
        expected.add(ImmutableList.of("[0.6, 0.8)", "20", "60", "69", "78"));
        expected.add(ImmutableList.of("[0.8, 1.0)", "20", "80", "89", "98"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_optional yesterday today as A, quantiles_optional as B group by quantiles(f, 5) select count(), field_min(f), f / count(), field_max(f)", true);
    }

    @Test
    public void testNoDocs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[0.0, 1.0)", "0", "NaN", "NaN", "NaN"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from quantiles_mandatory yesterday today as A, quantiles_mandatory as B where f<0 group by quantiles(f, 5) select count(), field_min(f), f / count(), field_max(f)", true);
    }

}
