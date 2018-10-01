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
import com.indeed.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GroupByPostAggregateTest extends BasicTest {
    final Dataset dataset = AllData.DATASET;

    @Test
    public void groupByFieldHavingDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk HAVING DISTINCT(oji) > 1", true);
    }

    @Test
    public void groupByFieldHavingMultipleConditions() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk HAVING DISTINCT(oji) > 1 AND COUNT() > 100", true);
    }

    @Test
    public void groupByTimeHavingDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by time(1h) HAVING DISTINCT(oji) > 1", true);
    }

    @Test
    public void groupByHavingFieldMax() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk[HAVING FIELD_MAX(oji) >= 100]", true);
    }

    @Test
    public void groupByMultipleField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "10", "3"));
        expected.add(ImmutableList.of("c", "1000", "1"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk, oji HAVING PARENT(FIELD_MAX(oji)) > 100", true);
    }
}
