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
import com.indeed.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author zheli
 */

public class BasicLuceneFilterTest extends BasicTest {
    final Dataset dataset = AllData.DATASET;

    @Test
    public void testBasicLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where lucene(\"oji:3\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where lucene(\"OJI:3\") select count()", true);

        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:a\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tk:b\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:c\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "141")), "from organic yesterday today where lucene(\"TK:d\") select count()", true);
    }

    @Test
    public void testBooleanLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"oji:3 OR OJI:4\") select count()", true);

        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "6")), "from organic yesterday today where lucene(\"TK:a OR TK:b\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "0")), "from organic yesterday today where lucene(\"TK:a AND TK:b\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "10")), "from organic yesterday today where lucene(\"TK:a OR TK:b OR TK:c\") select count()", true);
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:a\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tK:b\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"Tk:c\") select count()", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "143")), "from organic yesterday today where lucene(\"tk:d OR Tk:b\") select count()", true);
    }
}
