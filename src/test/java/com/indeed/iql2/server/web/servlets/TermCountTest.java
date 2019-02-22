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

public class TermCountTest extends BasicTest {

    private static String makeQuery(
            final boolean isIntField,
            final boolean isIntOperator,
            final int count) {
        final String fieldName = isIntField ? "intField" : "strField";
        final String operator = isIntOperator ? "inttermcount" : "strtermcount";
        return "from termCount yesterday today where " + operator + "(" + fieldName + ") = " + count;
    }

    private static void testIt(
            final boolean isIntField,
            final boolean isIntOperator) throws Exception {
        final List<List<String>> expected1 = new ArrayList<>();
        expected1.add(ImmutableList.of("", "33"));
        QueryServletTestUtils.testIQL2(expected1, makeQuery(isIntField, isIntOperator, 1), true);

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "50"));
        QueryServletTestUtils.testIQL2(expected2, makeQuery(isIntField, isIntOperator, 2), true);

        final List<List<String>> expected3 = new ArrayList<>();
        expected3.add(ImmutableList.of("", "17"));
        QueryServletTestUtils.testIQL2(expected3, makeQuery(isIntField, isIntOperator, 3), true);
    }

    @Test
    public void testIntTermCountIntField() throws Exception {
        testIt(true, true);
    }

    @Test
    public void testStrTermCountStrField() throws Exception {
        testIt(false, false);
    }

    @Test
    public void testIntTermCountStrField() throws Exception {
        testIt(false, true);
    }

    @Test
    public void testStrTermCountIntField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        // strtermcount(intfield) is always zero.
        expected.add(ImmutableList.of("", "0"));
        QueryServletTestUtils.testIQL2(expected, makeQuery(true, false, 1), true);
        QueryServletTestUtils.testIQL2(expected, makeQuery(true, false, 2), true);
        QueryServletTestUtils.testIQL2(expected, makeQuery(true, false, 3), true);
    }

}
