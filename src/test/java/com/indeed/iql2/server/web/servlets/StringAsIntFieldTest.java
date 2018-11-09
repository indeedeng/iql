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

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testWarning;

/**
 *
 */
public class StringAsIntFieldTest {
    final Dataset dataset = AllData.DATASET;

    @Test
    public void testSelectStringAsIntField() throws Exception {
        testWarning(dataset, ImmutableList.of("Field \"page\" in Dataset \"stringAsInt1\" is a string field but it is used as an int field in [HasInt{field={stringAsInt1=page}, term=0}]"),
                "from stringAsInt1 yesterday today SELECT page = 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"vp\" in Dataset \"stringAsInt1\" is a string field but it is used as an int field in [HasInt{field={stringAsInt1=vp}, term=0}]"),
                "from stringAsInt1 yesterday today SELECT vp != 0", QueryServletTestUtils.LanguageVersion.IQL2);

        testWarning(dataset, ImmutableList.of(), "from stringAsInt1 yesterday today SELECT vp != 0", QueryServletTestUtils.LanguageVersion.IQL1);
        testWarning(dataset, ImmutableList.of(), "from mobsearch yesterday today SELECT page = 0");
    }

    @Test
    public void testFilterStringAsIntField() throws Exception {
        testWarning(dataset, ImmutableList.of("Field \"page\" in Dataset \"stringAsInt1\" is a string field but it is used as an int field in " +
                        "[QueryAction{perDatasetQuery={stringAsInt1=int:page:0}, targetGroup=1, positiveGroup=1, negativeGroup=0}]"),
                "from stringAsInt1 yesterday today where page = 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"vp\" in Dataset \"stringAsInt1\" is a string field but it is used as an int field in " +
                        "[QueryAction{perDatasetQuery={stringAsInt1=int:vp:0}, targetGroup=1, positiveGroup=0, negativeGroup=1}]"),
                "from stringAsInt1 yesterday today where vp != 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"vp\" in Dataset \"stringAsInt1\" is a string field but it is used as an int field in " +
                        "[IntOrAction{field={stringAsInt1=vp}, terms=[2, 3, 1], targetGroup=1, positiveGroup=1, negativeGroup=0}]"),
                "from stringAsInt1 yesterday today where vp in (1, 2, 3)", QueryServletTestUtils.LanguageVersion.IQL2);

        testWarning(dataset, ImmutableList.of(), "from stringAsInt1 yesterday today where vp != 0", QueryServletTestUtils.LanguageVersion.IQL1);
        testWarning(dataset, ImmutableList.of(), "from stringAsInt1 yesterday today where vp in (1, 2, 3)", QueryServletTestUtils.LanguageVersion.IQL1);
        testWarning(dataset, ImmutableList.of(), "from mobsearch yesterday today where page = 0");
    }
}
