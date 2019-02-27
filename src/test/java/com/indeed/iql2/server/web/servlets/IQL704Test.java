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

import org.junit.Test;

public class IQL704Test extends BasicTest {
    @Test
    public void ensureErrorThrown() throws Exception {
        QueryServletTestUtils.expectExceptionAll("from big yesterday today group by field, field",
                QueryServletTestUtils.Options.create(true).setSubQueryTermLimit(5000L),
                s -> s.contains("Number of groups [5001] exceeds the group limit [5000]"));
    }
}
