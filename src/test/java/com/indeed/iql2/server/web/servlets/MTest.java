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

public class MTest extends BasicTest {
    @Test
    public void test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "1", "0", "151", "0", "4", "2", "1", "0", "6", "1"));
        QueryServletTestUtils.testIQL2(
                expected,
                "from organic yesterday today select count(), m(true), m(false), [m(true)], [m(false)], [m(tk='a')], [m(tk='b')], m(count()=151), [m(tk='a' and tk='b')], [m(tk='a' or tk='b')], m(count()>1 and tk='a'>0)"
        );
    }
}
