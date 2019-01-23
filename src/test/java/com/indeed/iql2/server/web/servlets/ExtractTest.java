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

public class ExtractTest extends BasicTest {
    @Test
    public void testBasic() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("5", "5", "3", "2", "10"));
        expected.add(ImmutableList.of("7", "7", "2", "5", "0"));
        QueryServletTestUtils.testIQL2(expected, "from extract yesterday today group by field2 select extract(field2, \"(\\\\d+)\"), extract(field1, \"a (\\\\d) (\\\\d)\"), extract(field1, \"a (\\\\d) (\\\\d)\", 2), extract(field3, \"(\\\\d+)\")", true);
    }
}
