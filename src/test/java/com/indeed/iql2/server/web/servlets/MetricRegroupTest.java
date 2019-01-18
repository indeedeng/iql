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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MetricRegroupTest extends BasicTest {
    @Test
    public void testMetricRegroupSingles() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[1, 2)", "84", "84"));
        expected.add(ImmutableList.of("[2, 3)", "1", "2"));
        expected.add(ImmutableList.of("[3, 4)", "60", "180"));
        expected.add(ImmutableList.of("[4, 5)", "0", "0"));
        expected.add(ImmutableList.of("[5, 6)", "1", "5"));
        expected.add(ImmutableList.of("[6, 7)", "0", "0"));
        expected.add(ImmutableList.of("[7, 8)", "0", "0"));
        expected.add(ImmutableList.of("[8, 9)", "0", "0"));
        expected.add(ImmutableList.of("[9, 10)", "0", "0"));
        expected.add(ImmutableList.of("[10, 11)", "2", "20"));
        expected.add(ImmutableList.of("< 1", "2", "0"));
        expected.add(ImmutableList.of(">= 11", "1", "15"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by bucket(ojc, 1, 11, 1) select count(), ojc");
    }

    @Test
    public void testMetricRegroupSinglesWithDefault() throws Exception {
        // TODO: Is inadvertently introducing WITH DEFAULT to iql1 bad?
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[1, 2)", "84", "84"));
        expected.add(ImmutableList.of("[2, 3)", "1", "2"));
        expected.add(ImmutableList.of("[3, 4)", "60", "180"));
        expected.add(ImmutableList.of("[4, 5)", "0", "0"));
        expected.add(ImmutableList.of("[5, 6)", "1", "5"));
        expected.add(ImmutableList.of("[6, 7)", "0", "0"));
        expected.add(ImmutableList.of("[7, 8)", "0", "0"));
        expected.add(ImmutableList.of("[8, 9)", "0", "0"));
        expected.add(ImmutableList.of("[9, 10)", "0", "0"));
        expected.add(ImmutableList.of("[10, 11)", "2", "20"));
        expected.add(ImmutableList.of("DEFAULT", "3", "15"));
        // IQL1 does not support regroup with default
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from organic yesterday today group by bucket(ojc, 1, 11, 1) with default select count(), ojc");
    }

    @Test
    public void testMetricRegroupIntervals() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[1, 3)", "85", "86"));
        expected.add(ImmutableList.of("[3, 5)", "60", "180"));
        expected.add(ImmutableList.of("[5, 7)", "1", "5"));
        expected.add(ImmutableList.of("[7, 9)", "0", "0"));
        expected.add(ImmutableList.of("[9, 11)", "2", "20"));
        expected.add(ImmutableList.of("< 1", "2", "0"));
        expected.add(ImmutableList.of(">= 11", "1", "15"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by bucket(ojc, 1, 11, 2) select count(), ojc");
    }

    @Test
    public void testMetricRegroupIntervalsWithDefault() throws Exception {
        // TODO: Is inadvertently introducing WITH DEFAULT to iql1 bad?
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[1, 3)", "85", "86"));
        expected.add(ImmutableList.of("[3, 5)", "60", "180"));
        expected.add(ImmutableList.of("[5, 7)", "1", "5"));
        expected.add(ImmutableList.of("[7, 9)", "0", "0"));
        expected.add(ImmutableList.of("[9, 11)", "2", "20"));
        expected.add(ImmutableList.of("DEFAULT", "3", "15"));
        // IQL1 does not support regroup with default
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from organic yesterday today group by bucket(ojc, 1, 11, 2) with default select count(), ojc");
    }

    @Test
    public void testRegroupBetweeen() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "3", "15"));
        expected.add(ImmutableList.of("1", "148", "291"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by between(ojc, 1, 11) select count(), ojc");
    }

    @Test
    public void testRegroupLucene() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
                ImmutableList.of("0", "15"),
                ImmutableList.of("1", "136"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by lucene(\"oji:[1 TO 10]\") select count()", true);
    }

    @Test
    public void invalidBucketSize() {
        final Predicate<String> containsBucketErrorMessage = e -> (e.contains("Bucket range should be a multiple of the interval"));
        QueryServletTestUtils.expectExceptionAll("FROM organic yesterday today GROUP BY bucket(oji,1,100,10) select count()", containsBucketErrorMessage);
        QueryServletTestUtils.expectExceptionAll("FROM organic yesterday today GROUP BY bucket(oji,1,95,10) select count()", containsBucketErrorMessage);
        QueryServletTestUtils.expectExceptionAll("FROM organic yesterday today GROUP BY bucket(oji,1,99,10) select count()", containsBucketErrorMessage);
    }

    @Test
    public void testRegroupTopTerms() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
                ImmutableList.of("1", "84"),
                ImmutableList.of("3", "60"));
        QueryServletTestUtils.testIQL1(
                expected,
                "from organic yesterday today group by topterms(ojc, 2, count()) select count()", true);
        QueryServletTestUtils.testIQL1(
                QueryServletTestUtils.addConstantColumn(1, "1", expected),
                "from organic yesterday today group by topterms(ojc, 2, count()), allbit select count()", true);
    }

    @Test
    public void testRegroupByDiff() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
                ImmutableList.of("d", "1212", "1473", "261"),
                ImmutableList.of("c", "1009", "1030", "21"),
                ImmutableList.of("b", "93", "110", "17"),
                ImmutableList.of("a", "33", "40", "7"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from organic yesterday today group by diff(tk, oji, ojc, 10) select count()", true);
    }
}
