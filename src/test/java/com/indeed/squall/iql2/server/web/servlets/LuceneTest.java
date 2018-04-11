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

package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author zheli
 */

public class LuceneTest extends BasicTest {
    final Dataset dataset = OrganicDataset.create();

    @Test
    public void testBasic() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today select lucene(\"oji:3\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"tk:a\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select lucene(\"tk:b\")", true);
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today select lucene(\"OJI:3\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today select lucene(\"Oji:3\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"tk:a\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select lucene(\"tK:b\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today select lucene(\"Tk:c\")", true);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "143")), "from organic yesterday today select lucene(\"tk:d OR Tk:b\")", true);
    }
}
