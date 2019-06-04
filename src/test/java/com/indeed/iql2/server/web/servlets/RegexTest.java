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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RegexTest {
    @Test
    public void testNormalDocFilter1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today where tk =~ \"a\" select count()");
    }

    @Test
    public void testNormalDocFilter2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today where tk =~ \"(b|c)\" select count()");
    }

    @Test
    public void testNormalDocMetric1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today select tk =~ \"a\"");
    }

    @Test
    public void testNormalDocMetric2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "6"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today select tk =~ \"(b|c)\"");
    }

    @Test
    public void testAggregateDocFilter() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk having tk =~ \"(a|b|c)\" > 0 select count()");
    }

    @Test
    public void testInvalid() throws Exception {
        QueryServletTestUtils.expectExceptionAll(
                "from organic yesterday today where tk =~ \"[]*\" select count()",
                ex -> (ex.contains("The provided regex filter") && ex.contains("failed to parse")));
    }

    @Test
    public void testExpensive() throws Exception {
        QueryServletTestUtils.expectExceptionAll(
                "from organic yesterday today where tk =~ \".*ios.*|.*software.*|.*web.*|.*java.*|.*hadoop.*|.*spark.*|.*nlp.*|.*algorithm.*|.*python.*|.*matlab.*|.*swift.*|.*android.*\" select count()",
                ex -> ex.contains("The provided regex is too complex."));
    }

    @Test
    public void testWithoutQuotes() throws Exception {
        final String[] filters = new String[] {"tk =~ a", "tk !=~ a", "tk =~ 10", "tk !=~ 10"};
        final long[] counts = new long[] {4, 147, 0, 151};
        for (int i = 0; i < filters.length; i++) {
            final String query = "from organic yesterday today where " + filters[i] + " select count()";
            final List<List<String>> expected = new ArrayList<>();
            expected.add(ImmutableList.of("", Long.toString(counts[i])));
            QueryServletTestUtils.testIQL1(expected, query);

            QueryServletTestUtils.expectException(
                    query,
                    QueryServletTestUtils.LanguageVersion.IQL2,
                    s -> s.contains("IqlKnownException$ParseErrorException"));
        }
    }
}
