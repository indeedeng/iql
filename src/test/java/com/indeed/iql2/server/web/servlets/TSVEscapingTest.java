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
import com.indeed.iql2.server.web.servlets.dataset.TSVEscapeDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TSVEscapingTest extends BasicTest {

    @Test
    public void testStreamingLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "1"));
        expected.add(ImmutableList.of("NormalTerm", "1"));
        QueryServletTestUtils.testAll(TSVEscapeDataset.createDataset(), expected, "from tsvescape yesterday today group by sField", true);
    }

    @Test
    public void testStreamingPreviousRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("NormalTerm", "2", "1"));
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "3", "1"));
        QueryServletTestUtils.testAll(TSVEscapeDataset.createDataset(), expected, "from tsvescape yesterday today group by sField, iField", true);
    }

    @Test
    public void testStreamingPreviousRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "1"));
        expected.add(ImmutableList.of("NormalTerm", "NormalTerm", "1"));
        QueryServletTestUtils.testAll(TSVEscapeDataset.createDataset(), expected, "from tsvescape yesterday today group by sField, sField", true);
    }

    @Test
    public void testGetGroupStats() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Crazy\uFFFDTerm\uFFFD!\uFFFD\uFFFD", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "1"));
        expected.add(ImmutableList.of("NormalTerm", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "1"));
        QueryServletTestUtils.testAll(TSVEscapeDataset.createDataset(), expected, "from tsvescape yesterday today group by sField, time(1d)", true);
    }
}
