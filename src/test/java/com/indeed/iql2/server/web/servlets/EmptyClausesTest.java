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

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author jwolfe
 */

public class EmptyClausesTest extends BasicTest {
    @Test
    public void emptyVariations() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151"));
        testAll(expected, "from organic yesterday today");
        testAll(expected, "from organic yesterday today where");
        testAll(expected, "from organic yesterday today where group by");
        testAll(expected, "from organic yesterday today where group by select count()");
        testAll(expected, "from organic yesterday today where select count()");
        testAll(expected, "from organic yesterday today select count()");
        testAll(expected, "from organic yesterday today group by");
        testAll(expected, "from organic yesterday today group by select count()");
    }

    // Behavior changed in IQL-610 to make empty SELECT act as SELECT COUNT()
    @Test
    public void emptySelectIsCountSimple() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151"));
        testAll(expected, "from organic yesterday today select");
        testAll(expected, "from organic yesterday today where select");
        testAll(expected, "from organic yesterday today where group by select");
        testAll(expected, "from organic yesterday today group by select");
        testAll(expected, "select from organic yesterday today");
        testAll(expected, "select from organic yesterday today where");
        testAll(expected, "select from organic yesterday today where group by");
   }

    @Test
    public void selectFromAliased() throws Exception {
        // This test is not strictly about empty clauses, but
        // about disambiguating between empty select (select from)
        // and select from where from is an aliased field name
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "2653"));
        testAll(expected, "select from from organic yesterday today aliasing (oji as from)", true);
    }

    @Test
    public void emptySelectIsCountWithGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        testAll(expected, "from organic yesterday today where group by tk select");
    }

    @Test
    public void emptySelectIsCountWithWhere() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "129"));
        testAll(expected, "from organic yesterday today where oji = 10 select");

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "141"));
        testAll(expected2, "from organic yesterday today where tk = 'd' select");
    }

    @Test
    public void emptySelectIsCountWithWhereAndGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "1"));
        expected.add(ImmutableList.of("c", "3"));
        expected.add(ImmutableList.of("d", "121"));
        testAll(expected, "from organic yesterday today where oji = 10 group by tk select");
    }

    @Test
    public void testGroupBySelect() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("a", "1"),
                ImmutableList.of("b", "1")
        );
        testAll(expected, "from groupBySelect yesterday today group by `select`");
    }
}
