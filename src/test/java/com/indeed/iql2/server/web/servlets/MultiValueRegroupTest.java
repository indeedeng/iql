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
import com.indeed.iql2.server.web.servlets.dataset.MultiValueDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MultiValueRegroupTest extends BasicTest {
    @Test
    public void testTopKLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(AllData.DATASET, expected, "from multiValue yesterday today group by f[2] select count()", true);
    }

    @Test
    public void testFieldInLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(AllData.DATASET, expected, "from multiValue yesterday today group by f in (1,2) select count()", true);
    }

    @Test
    public void testGroupByMultiValueInIQL1() throws Exception {
        QueryServletTestUtils.testIQL1(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("1", "50")),
                "from multiValue yesterday today where f in (1,2) i=1 GROUP BY f", true);
        QueryServletTestUtils.testIQL1(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("1", "1", "50")),
                "from multiValue yesterday today where f in (1) f in (2) GROUP BY f, f", true);
        QueryServletTestUtils.testIQL1(AllData.DATASET,
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50")
                ),
                "from multiValue yesterday today where f in (1,2) GROUP BY i, f", true);
        QueryServletTestUtils.testIQL1(AllData.DATASET,
                ImmutableList.of(),
                "from multiValue yesterday today where f in (1) f in (2) i = 1 GROUP BY f", true);
    }

    @Test
    public void testGroupByMultiValueInIQL2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "50"));
        expected.add(ImmutableList.of("3", "17"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from multiValue yesterday today where f in (1,2) i=1 GROUP BY f", true);

        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50"),
                        ImmutableList.of("0", "3", "17"),
                        ImmutableList.of("1", "3", "17")
                ),
                "from multiValue yesterday today where f in (1,2) GROUP BY i, f", true);
    }

}
