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
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.iql2.server.web.servlets.dataset.FieldEqualDataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class FieldEqualMetricTest extends BasicTest {

    @Test
    public void testEqualFieldMetric() throws Exception {
        final Dataset dataset = FieldEqualDataset.create();
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select s1=s2");
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "3", "4")), "from organic yesterday today select i1=i2, count()");
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2", "3", "4")), "from organic yesterday today select s1=s2, i1=i2, count()");

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1"));
        expected.add(ImmutableList.of("b", "1"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by s1 select s1=s2");
    }

    @Test
    public void testMultiDatasetEqualFieldMetric() throws Exception {
        try {
            QueryServletTestUtils.testIQL2(FieldEqualDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "fail")), "from organic yesterday today as o1, organic yesterday today as o2 select o1.i1=o2.i2");
            Assert.fail("field on different dataset should throw exception");
        } catch (Exception e) {
        }
    }

    @Test
    public void testNotEqualFieldMetric() throws Exception {
        final Dataset dataset = FieldEqualDataset.create();
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today select i1!=i2");
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select s1!=s2");
        QueryServletTestUtils.testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2", "1")), "from organic yesterday today select s1!=s2, i1!=i2");
    }
}
