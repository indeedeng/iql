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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldExtremaTest extends BasicTest {
    @Test
    public void testBasicResult() throws Exception {
        final List<String> expected = ImmutableList.of("", "3", "1000", "0", "15");
        QueryServletTestUtils.testIQL2(
            ImmutableList.of(expected),
            "from organic yesterday today select FIELD_MIN(oji), FIELD_MAX(oji), FIELD_MIN(ojc), FIELD_MAX(ojc)",
            true
        );
    }

    @Test
    public void testByStatement() throws Exception {
        final List<String> expected = ImmutableList.of("", "3", "10", "2", "1");
        QueryServletTestUtils.testIQL2(
            ImmutableList.of(expected),
            "from organic yesterday today select "
            + "FIELD_MIN(oji by count()), FIELD_MAX(oji by count()), "
            + "FIELD_MIN(ojc by count()), FIELD_MAX(ojc by count())",
            true
        );
    }

    @Test
    public void testHavingStatement() throws Exception {
        final List<String> expected = ImmutableList.of("", "3", "1000", "0", "10");
        QueryServletTestUtils.testIQL2(
            ImmutableList.of(expected),
            "from organic yesterday today select "
            + "FIELD_MIN(oji having avg(ojc)=1), FIELD_MAX(oji having avg(ojc)=1), "
            + "FIELD_MIN(ojc having avg(oji)=10), FIELD_MAX(ojc having avg(oji)=10)",
            true
        );
    }

    @Test
    public void testWithGroupBy() throws Exception {
        final List<List<String>> expected = ImmutableList.of(
            ImmutableList.of("0", "10", "10"),
            ImmutableList.of("1", "3", "1000"),
            ImmutableList.of("2", "10", "10"),
            ImmutableList.of("3", "10", "10"),
            ImmutableList.of("5", "10", "10"),
            ImmutableList.of("10", "10", "10"),
            ImmutableList.of("15", "100", "100")
        );
        QueryServletTestUtils.testIQL2(
            expected,
            "from organic yesterday today group by ojc select FIELD_MIN(oji), FIELD_MAX(oji)",
            true
        );
    }

    @Test
    public void testWithNaN() throws Exception {
        final List<String> expected = ImmutableList.of("", "NaN", "NaN", "NaN", "NaN");
        QueryServletTestUtils.testIQL2(
            ImmutableList.of(expected),
            "from organic yesterday today select "
            + "FIELD_MIN(oji having false), FIELD_MAX(oji having false), "
            + "FIELD_MIN(ojc having false), FIELD_MAX(ojc having false)",
            true
        );       
    }

    @Test
    public void testStringField() throws Exception {
        final List<String> expected = ImmutableList.of("", "NaN", "NaN", "NaN", "NaN");
        QueryServletTestUtils.testIQL2(
            ImmutableList.of(expected),
            "from organic yesterday today select "
            + "FIELD_MIN(country), FIELD_MAX(country), "
            + "FIELD_MIN(country), FIELD_MAX(country)",
            true
        );       
    }
}
