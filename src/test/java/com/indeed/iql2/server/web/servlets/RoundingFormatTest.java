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
import com.indeed.iql2.server.web.servlets.dataset.JobsearchDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanlei
 */

public class RoundingFormatTest extends BasicTest {
    @Test
    public void testSingleSelectRounding() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("cn", "1.00"));
        expected.add(ImmutableList.of("gb", "2.00"));
        expected.add(ImmutableList.of("jp", "2.00"));
        expected.add(ImmutableList.of("uk", "2.00"));
        expected.add(ImmutableList.of("us", "3.00"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country select count() rounding 2");
    }

    @Test
    public void testMultipleSelectRounding() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("cn", "1.00", "0.50"));
        expected.add(ImmutableList.of("gb", "2.00", "1.00"));
        expected.add(ImmutableList.of("jp", "2.00", "1.00"));
        expected.add(ImmutableList.of("uk", "2.00", "1.00"));
        expected.add(ImmutableList.of("us", "3.00", "1.50"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country select count(), count() / 2 rounding 2");
    }

    @Test
    public void testSelectPrintfRounding() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("cn", "1.00", "1"));
        expected.add(ImmutableList.of("gb", "2.00", "2"));
        expected.add(ImmutableList.of("jp", "2.00", "2"));
        expected.add(ImmutableList.of("uk", "2.00", "2"));
        expected.add(ImmutableList.of("us", "3.00", "3"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country select printf(\"%.2f\", count()), count() rounding 0");
    }

    @Test
    public void testSelectRoundingMissingN() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        try {
            QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country select count() rounDing");
        } catch (final Exception e){
            //expected
        }
    }

    @Test
    public void testSelectPrintfNoRounding() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("cn", "1.00", "1"));
        expected.add(ImmutableList.of("gb", "2.00", "2"));
        expected.add(ImmutableList.of("jp", "2.00", "2"));
        expected.add(ImmutableList.of("uk", "2.00", "2"));
        expected.add(ImmutableList.of("us", "3.00", "3"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country select printf(\"%.2f\", count()), count()");
    }
}
