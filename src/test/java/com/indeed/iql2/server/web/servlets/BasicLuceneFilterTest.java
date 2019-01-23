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
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2AndLegacy;

/**
 * @author zheli
 */

public class BasicLuceneFilterTest extends BasicTest {
    @Test
    public void testBasicLuceneFilters() throws Exception {
        testAll(ImmutableList.of(ImmutableList.of("", "1")), "from organic yesterday today where lucene(\"oji:3\") select count()", true);

        testAll(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:a\") select count()", true);
        testAll(ImmutableList.of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tk:b\") select count()", true);
        testAll(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:c\") select count()", true);
        testAll(ImmutableList.of(ImmutableList.of("", "141")), "from organic yesterday today where lucene(\"tk:d\") select count()", true);
    }

    @Test
    public void testBooleanLuceneFilters() throws Exception {
        testAll(ImmutableList.of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"oji:3 OR oji:4\") select count()", true);

        testAll(ImmutableList.of(ImmutableList.of("", "6")), "from organic yesterday today where lucene(\"tk:a OR tk:b\") select count()", true);
        testAll(ImmutableList.of(ImmutableList.of("", "0")), "from organic yesterday today where lucene(\"tk:a AND tk:b\") select count()", true);
        testAll(ImmutableList.of(ImmutableList.of("", "10")), "from organic yesterday today where lucene(\"tk:a OR tk:b OR tk:c\") select count()", true);
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        // Iql1 is case sensitive
        testIQL2AndLegacy(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:a\") select count()", true);
        testIQL2AndLegacy(ImmutableList.of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tK:b\") select count()", true);
        testIQL2AndLegacy(ImmutableList.of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"Tk:c\") select count()", true);
        testIQL2AndLegacy(ImmutableList.of(ImmutableList.of("", "143")), "from organic yesterday today where lucene(\"tk:d OR Tk:b\") select count()", true);
    }
}
