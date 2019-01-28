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
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ValidationTests extends BasicTest {
    @Test
    public void testBasicValidationPassing() throws Exception {
        final String basic =
                "FROM trivialOrganic 2015-01-01 2015-01-02, trivialSponsored " +
                "SELECT COUNT(), trivialOrganic.COUNT(), trivialSponsored.COUNT(), clicked, trivialOrganic.clicked, trivialSponsored.clicked, [trivialOrganic.clicked], [trivialSponsored.clicked]";
        final String aliases =
                "FROM trivialOrganic 2015-01-01 2015-01-02 as o, trivialSponsored as s " +
                        "SELECT COUNT(), o.COUNT(), s.COUNT(), clicked, o.clicked, s.clicked, [o.clicked], [s.clicked]";
        final String sameDataset =
                "FROM trivialOrganic 2015-01-01 2015-01-02 as o1, trivialOrganic as o2 " +
                        "SELECT COUNT(), o1.COUNT(), o2.COUNT(), clicked, o1.clicked, o2.clicked, [o1.clicked], [o2.clicked]";

        final List<List<String>> expected = ImmutableList.of(Arrays.asList("", "2", "1", "1", "2", "1", "1", "1", "1"));

        for (final String query : Arrays.asList(basic, aliases, sameDataset)) {
            QueryServletTestUtils.testIQL2(expected, query);
        }
    }

    @Test
    public void testBasicValidationRejecting() {
        final String query =
                "FROM trivialOrganic 2015-01-01 2015-01-02, trivialSponsored " +
                "SELECT [trivialOrganic.clicked + trivialSponsored.clicked]";
        try {
            QueryServletTestUtils.runQuery(query, QueryServletTestUtils.LanguageVersion.IQL2, false, QueryServletTestUtils.Options.create(), Collections.emptySet());
            Assert.fail();
        } catch (Exception ignored) {
        }
    }
}
