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

import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.runAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.runIQL2;

public class ExplainTest extends BasicTest {
    @Test
    public void testExplain() throws Exception {
        runAll(AllData.DATASET.getShards(), "explain from organic yesterday today where tk=\"a\" select count()");
        runAll(AllData.DATASET.getShards(), "explain from organic yesterday today group by tk[5], oji, ojc select count()");
    }

    @Test
    public void testSubqueryExplain() throws Exception {
        runIQL2(AllData.DATASET.getShards(), "explain from organic yesterday today as o1 where o1.tk in (from same group by tk) and oji in (from same where oji=1 group by ojc) group by tk select count()");
    }
}
