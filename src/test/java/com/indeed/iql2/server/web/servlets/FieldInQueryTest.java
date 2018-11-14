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

public class FieldInQueryTest extends BasicTest {
    @Test
    public void fieldInQueryStrings() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET, expected,
                "from organic yesterday today where tk in (from other1 1d 0d group by thefield) group by tk with default select count(), distinct(tk)", true);
    }

    @Test
    public void fieldNotInQueryStrings() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "4", "1"));
        expected.add(ImmutableList.of("d", "141", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET, expected,
                "from organic yesterday today where tk not in (from other2 1d 0d group by thefield) group by tk with default select count(), distinct(tk)", true);
    }

    // Test for IQL-616
    @Test
    public void groupByFieldInQuery() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "4", "1"));
        expected.add(ImmutableList.of("d", "141", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET, expected,
                "from organic yesterday today group by ojc in (from manyValues 1d 0d group by thefield) select count()", true);
    }

    @Test
    public void fieldInQueryInts() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "3"));
        expected.add(ImmutableList.of("10", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET, expected,
                "from organic yesterday today where ojc in (from other3 1d 0d group by thefield) group by ojc with default select count(), distinct(tk)", true);
    }

    @Test
    public void fieldNotInQueryInts() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("3", "60", "1"));
        expected.add(ImmutableList.of("5", "1", "1"));
        expected.add(ImmutableList.of("15", "1", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET, expected,
                "from organic yesterday today where ojc not in (from other4 1d 0d group by thefield) group by ojc with default select count(), distinct(tk)", true);
    }

}
