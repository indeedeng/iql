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
import java.util.function.Predicate;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.addConstantColumn;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2AndLegacy;

public class FieldRegroupTest extends BasicTest {
    @Test
    public void testBasicGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("15", "1", "15"));
        testAll(expected, "from organic yesterday today group by ojc select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc, allbit select count(), ojc", true);
    }

    @Test
    public void testFirstK() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        testAll(expected, "from organic yesterday today group by ojc select count(), ojc LIMIT 3", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc, allbit select count(), ojc LIMIT 3", true);
    }

    @Test
    public void testFieldInInt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        testAll(expected, "from organic yesterday today group by ojc in (1, 2) select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc in (1, 2), allbit select count(), ojc", true);
    }

    @Test
    public void testFieldInSting() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "7"));
        expected.add(ImmutableList.of("b", "2", "17"));
        testAll(expected, "from organic yesterday today group by tk in (\"a\", \"b\") select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by tk in (\"a\", \"b\"), allbit select count(), ojc", true);
    }

    @Test
    public void testImplicitOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("0", "2", "0"));
        testAll(expected, "from organic yesterday today group by ojc[3] select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[3], allbit select count(), ojc", true);
    }

    @Test
    public void testImplicitOrderingBackwardsIntField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("2", "1", "2"));
        testIQL2AndLegacy(expected, "from organic yesterday today group by ojc[BOTTOM 3] select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[bottom 3], allbit select count(), ojc", true);
    }

    @Test
    public void testImplicitOrderingBackwardsStringField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "1", "2"));
        expected.add(ImmutableList.of("c", "1", "4"));
        expected.add(ImmutableList.of("a", "1", "4"));
        testAll(expected, "from organic yesterday today group by tk[bottom 3], allbit select count()", true);
    }

    @Test
    public void testIntTopKOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        testAll(expected, "from organic yesterday today group by ojc[100 by ojc/count()] select count(), ojc", true);
        testAll(ImmutableList.of(
                ImmutableList.of("1", "1", "84", "84"),
                ImmutableList.of("3", "1", "60", "180")),
                "from organic yesterday today group by ojc[2 by count()], allbit select count(), ojc", true);
    }

    @Test
    public void testStringTopKOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141", "261"));
        expected.add(ImmutableList.of("c", "4", "21"));
        expected.add(ImmutableList.of("b", "2", "17"));
        expected.add(ImmutableList.of("a", "4", "7"));
        testAll(expected, "from organic yesterday today group by tk[100 by ojc] select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by tk[100 by ojc], allbit select count(), ojc", true);
    }

    @Test
    public void testTopKBottomOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("5", "1", "5"));
        testAll(expected, "from organic yesterday today group by ojc[bottom 3 by ojc] select count(), ojc", true);
        testAll(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[bottom 3 by ojc], allbit select count(), ojc", true);
    }

    @Test
    public void testOrderingOnly() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        // IQL1 does not support '[by metric]' sorting.
        testIQL2AndLegacy(expected, "from organic yesterday today group by ojc[BY ojc/count()] select count(), ojc", true);
        testIQL2(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[BY ojc/count()], allbit select count(), ojc", true);
    }

    @Test
    public void testImplicitOrderingLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("3", "60", "180"));
        // TODO: Introduce fully deterministic ordering for ties and increase to top 3?
        testAll(expected, "from organic yesterday today group by ojc[5] select count(), ojc limit 2", true);
        testIQL2(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[5], allbit select count(), ojc limit 2", true);
    }

    @Test
    public void testTopKOrderingLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        testAll(expected, "from organic yesterday today group by ojc[100 by ojc/count()] select count(), ojc limit 2", true);
        testIQL2(addConstantColumn(1, "1", expected), "from organic yesterday today group by ojc[100 BY ojc/count()], allbit select count(), ojc limit 2", true);
    }

    @Test
    public void testRandomFieldRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "0"));
        expected.add(ImmutableList.of("1", "8"));
        expected.add(ImmutableList.of("2", "3"));
        expected.add(ImmutableList.of("3", "140"));
        testIQL2(expected, "from organic yesterday today group by random(oji, 3, \"SomeRandomSalt\") select count()", true);
        testIQL2(addConstantColumn(1, "1", expected.subList(1, expected.size())), "from organic yesterday today group by random(oji, 3, \"SomeRandomSalt\"), allbit select count()", true);
    }

    @Test
    public void testRandomMetricRegroup1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "0"));
        expected.add(ImmutableList.of("1", "68"));
        expected.add(ImmutableList.of("2", "38"));
        expected.add(ImmutableList.of("3", "45"));
        testIQL2(expected, "from organic yesterday today group by random(docId(), 3, \"SomeRandomSalt\") select count()", true);
        testIQL2(addConstantColumn(1, "1", expected.subList(1, expected.size())), "from organic yesterday today group by random(docId(), 3, \"SomeRandomSalt\"), allbit select count()", true);
    }

    @Test
    public void testRandomMetricRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "0"));
        expected.add(ImmutableList.of("1", "70"));
        expected.add(ImmutableList.of("2", "72"));
        expected.add(ImmutableList.of("3", "9"));
        testIQL2(expected, "from organic yesterday today group by random(oji + ojc * 10, 3, \"SomeRandomSalt\") select count()", true);
        testIQL2(addConstantColumn(1, "1", expected.subList(1, expected.size())), "from organic yesterday today group by random(oji + ojc * 10, 3, \"SomeRandomSalt\"), allbit select count()", true);
    }

    @Test
    public void testRandomMetricRegroup3() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "0"));
        expected.add(ImmutableList.of("1", "134"));
        expected.add(ImmutableList.of("2", "10"));
        expected.add(ImmutableList.of("3", "7"));
        testIQL2(expected, "from organic yesterday today group by random(ojc * ojc + oji * strtermcount(tk) + LEN(tk) * (1 + oji) * (unixtime % 10 + 1 ), 3, \"SomeRandomSalt\") select count()", true);
        testIQL2(addConstantColumn(1, "1", expected.subList(1, expected.size())), "from organic yesterday today group by random(ojc * ojc + oji * strtermcount(tk) + LEN(tk) * (1 + oji) * (unixtime % 10 + 1 ), 3, \"SomeRandomSalt\"), allbit select count()", true);
    }

    @Test
    public void testRandomMetricInvalidMetric() {
        final Predicate<String> errorDuringValidation = e -> e.contains("Errors found when validating query");
        QueryServletTestUtils.expectException("FROM organic yesterday today GROUP BY RANDOM(EXTRACT(tk, \"+\"), 10)", QueryServletTestUtils.LanguageVersion.IQL2, errorDuringValidation);
    }
}
