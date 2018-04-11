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

package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RegexTest {
    @Test
    public void testNormalDocFilter1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"a\" select count()");
    }

    @Test
    public void testNormalDocFilter2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"(b|c)\" select count()");
    }

    @Test
    public void testNormalDocMetric1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select tk =~ \"a\"");
    }

    @Test
    public void testNormalDocMetric2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select tk =~ \"(b|c)\"");
    }

    @Test
    public void testAggregateDocFilter() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having tk =~ \"(a|b|c)\" > 0 select count()");
    }

    @Test
    public void testInvalid() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        try {
            QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \"[]*\" select count()");
            Assert.fail("Regex should not have parsed successfully.");
        } catch (Exception e) {
        }
    }

    @Test
    public void testExpensive() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        try {
            QueryServletTestUtils.testAll(OrganicDataset.create(), expected, "from organic yesterday today where tk =~ \".*ios.*|.*software.*|.*web.*|.*java.*|.*hadoop.*|.*spark.*|.*nlp.*|.*algorithm.*|.*python.*|.*matlab.*|.*swift.*|.*android.*\" select count()");
            Assert.fail("Regex should not have parsed successfully.");
        } catch (Exception e) {
        }
    }
}
