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
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DateTimeTest extends BasicTest {
    @Test
    public void testWordDate() throws Exception {
        testIQL1(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic y today select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "153", "2655", "308")), "from organic 3days ago select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic d ago select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testIQL1(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60M ago select count(), oji, ojc");
        testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 60M ago select count(), oji, ojc");
    }

    @Test
    public void testQuotes() throws Exception {
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic \"d\" \"today\" select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic \"1d\" today select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("", "160", "2662", "315")), "from organic \"10d\" \"today\" select count(), oji, ojc");
    }

    @Test
    public void testSingleDigitDates() throws  Exception {
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("","10")), "from organic 2015-1-1T0:0:0 2015-1-1 01:00:00 select count()");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("","60","600","60")), "from organic 2015-1-01 1:0:0 2015-01-1T2:00:0 select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("","19","190","57")), "from organic 2015-1-01 2:1:0 2015-1-01 2:19:4 select count(), oji, ojc");
        testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("","19","190","57")), "from organic 2015-1-01T2:1:0 2015-1-01 2:19:4 select count(), oji, ojc");
    }

}
