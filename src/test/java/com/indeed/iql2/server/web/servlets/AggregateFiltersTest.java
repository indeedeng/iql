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
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.iql2.server.web.servlets.dataset.JobsearchDataset;
import com.indeed.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class AggregateFiltersTest {
    @Test
    public void testMetricIs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() = 4 select count()");
    }

    @Test
    public void testMetricIsRegex() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("uk", "2"));
        expected.add(ImmutableList.of("us", "3"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country having term() =~ \"u.*\" select count()");
    }

    @Test
    public void testMetricIsnt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() != 4 select count()");
    }

    @Test
    public void testMetricGte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() >= 4 select count()");
    }

    @Test
    public void testMetricGt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() > 4 select count()");
    }

    @Test
    public void testMetricLt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 select count()");
    }

    @Test
    public void testMetricLte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 2 select count()");
    }

    @Test
    public void testAnd() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 4 AND count() >= 4 select count()");
    }

    @Test
    public void testOr() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 OR count() > 4 select count()");
    }

    @Test
    public void testAlways() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having true select count()");
    }

    @Test
    public void testNever() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having false select count()");
    }

    @Test
    public void testRegex() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "84"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "60"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "2"));
        expected.add(ImmutableList.of("15", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by ojc having tk=~\".*\" select count()", true);
    }

    @Test
    public void testSample() throws Exception {
        final String[] metric = new String[] {"oji", "ojc", "docId()", "0", "oji + 10", "oji + ojc * LEN(tk)" };
        final int[] result = new int[] {142, 63, 63, 0, 139, 141};
        for (int i = 0; i < metric.length; i++) {
            final List<List<String>> expected = new ArrayList<>();
            expected.add(ImmutableList.of("", String.valueOf(result[i])));
            QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected,
                    "from organic yesterday today where sample("+ metric[i] + ", 1, 2, \"SomeRandomSalt\") select count()", true);
        }
    }
}
