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

public class MultiValueRegroupTest extends BasicTest {
    @Test
    public void testTopKLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(expected, "from multiValue yesterday today group by f[2] select count()", true);
    }

    @Test
    public void testFieldInLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(expected, "from multiValue yesterday today group by f in (1,2) select count()", true);
    }

    @Test
    public void testGroupByMultiValueInIQL1() throws Exception {
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("1", "50")),
                "from multiValue yesterday today where sf in (\"1\",\"2\") i=1 GROUP BY sf", true);
        // This query fails with ArrayIndexOutOfBoundsException in Iql1
        // and returns ("1", "1", "50") in legacy mode
        // But if you change filter to "where sf in ("2") sf in ("1")" it returns ("2", "2", "50")
        // Not sure it's worth to spend much time on that.
        // It's here just for history
        //QueryServletTestUtils.testIQL1(
        //        ImmutableList.of(ImmutableList.of("1", "1", "50")),
        //        "from multiValue yesterday today where sf in (\"1\") sf in (\"2\") GROUP BY sf, sf", true);
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50")
                ),
                "from multiValue yesterday today where sf in (\"1\",\"2\") GROUP BY i, sf", true);
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(),
                "from multiValue yesterday today where sf in (\"1\") sf in (\"2\") i = 1 GROUP BY sf", true);
    }

    @Test
    public void testRegroupByStringFieldWithIntFilter() throws Exception {
        // page is string multivalued field, some docs have "last" as a second term.
        // Check that in IQL1 string field optimization is applied even if we filter int values.
        final String query = "from jobsearch yesterday today where page in (1, 2, 3) group by page";
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(
                        ImmutableList.of("1", "2"),
                        ImmutableList.of("2", "2"),
                        ImmutableList.of("3", "2")),
                query);
        // Check that in IQL2 is without these optimization.
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(
                        ImmutableList.of("1", "2"),
                        ImmutableList.of("2", "2"),
                        ImmutableList.of("3", "2"),
                        ImmutableList.of("last", "3")
                ),
                query);
    }

    @Test
    public void testGroupByMultiValueInIQL2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "50"));
        expected.add(ImmutableList.of("3", "17"));
        QueryServletTestUtils.testIQL2(expected, "from multiValue yesterday today where f in (1,2) i=1 GROUP BY f", true);

        QueryServletTestUtils.testIQL2(
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50"),
                        ImmutableList.of("0", "3", "17"),
                        ImmutableList.of("1", "3", "17")
                ),
                "from multiValue yesterday today where f in (1,2) GROUP BY i, f", true);
    }

    @Test
    public void testNoOptimizeWithTopK() throws Exception {
        // Check that "where field in (..) group by field" -> "group by field in (..)" optimization
        // does not apply in topK regroups
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(
                        ImmutableList.of("1", "50"),
                        ImmutableList.of("3", "17")),
                "from multiValue yesterday today where sf in (\"1\",\"2\") i=1 GROUP BY sf[10]", true);
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(
                        ImmutableList.of("1", "251"),
                        ImmutableList.of("3", "119"),
                        ImmutableList.of("2", "117")
                ),
                "from multiValue yesterday today where sf in (\"1\",\"2\") GROUP BY sf[10 by f + i] select f + i", true);
    }

    @Test
    public void testTermSubsetOptimization() throws Exception {
        // test ordinary case (no terms subset or topK)
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(
                        ImmutableList.of("a", "50"),
                        ImmutableList.of("b", "50")),
                "from multiValue yesterday today where grp in (\"a\",\"b\") GROUP BY grp", true);
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(
                        ImmutableList.of("a", "50"),
                        ImmutableList.of("b", "50"),
                        ImmutableList.of("c", "36"),
                        ImmutableList.of("d", "36")),
                "from multiValue yesterday today where grp in (\"a\",\"b\") GROUP BY grp", true);

        // group by subset where subset matches filtering subset
        // optimization is still applied.
        QueryServletTestUtils.testAll(
                ImmutableList.of(
                        ImmutableList.of("a", "50"),
                        ImmutableList.of("b", "50")),
                "from multiValue yesterday today where grp in (\"a\",\"b\") GROUP BY grp in (\"a\",\"b\")", true);

        // group by subset where subset does not match filtering subset
        // optimization is omitted: it is filtering + group by term subset
        QueryServletTestUtils.testAll(
                ImmutableList.of(
                        ImmutableList.of("c", "36"),
                        ImmutableList.of("d", "36")),
                "from multiValue yesterday today where grp in (\"a\",\"b\") GROUP BY grp in (\"c\",\"d\")", true);
    }
}
