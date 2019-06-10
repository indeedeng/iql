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

package com.indeed.iql2.execution.groupkeys.sets;

import com.indeed.iql2.Formatter;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.query.UnevenGroupByPeriod;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

public class TestYearMonthGroupKeySet {

    private static final String FORMAT_STRING = TimeUnit.SECOND.formatString;
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern(FORMAT_STRING).withLocale(Locale.US);

    private static YearMonthGroupKeySet create() {
        final DumbGroupKeySet dumbGroupKeySet = DumbGroupKeySet.create(DumbGroupKeySet.empty(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new YearMonthGroupKeySet(dumbGroupKeySet, 12, new DateTime(2015, 2, 1, 0, 0, 0), UnevenGroupByPeriod.MONTH, FORMAT_STRING, Formatter.TSV);
    }

    @Test
    public void testParentGroup() throws Exception {
        final YearMonthGroupKeySet yearMonthGroupKey = create();
        for (int i = 1; i <= 12; i++) {
            Assert.assertEquals(1, yearMonthGroupKey.parentGroup(i));
        }
        for (int i = 13; i <= 24; i++) {
            Assert.assertEquals(2, yearMonthGroupKey.parentGroup(i));
        }
        for (int i = 25; i <= 36; i++) {
            Assert.assertEquals(3, yearMonthGroupKey.parentGroup(i));
        }
        for (int i = 37; i <= 48; i++) {
            Assert.assertEquals(4, yearMonthGroupKey.parentGroup(i));
        }
        for (int i = 49; i <= 60; i++) {
            Assert.assertEquals(5, yearMonthGroupKey.parentGroup(i));
        }
    }

    @Test
    public void testGroupKey() throws Exception {
        final Formatter formatter = Formatter.TSV;
        final YearMonthGroupKeySet yearMonthGroupKey = create();
        for (int i = 1; i <= 60; i+=12) {
            final long start = new DateTime(2015, 2, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 3, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 2; i <= 60; i+=12) {
            final long start = new DateTime(2015, 3, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 4, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 3; i <= 60; i+=12) {
            final long start = new DateTime(2015, 4, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 5, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 4; i <= 60; i+=12) {
            final long start = new DateTime(2015, 5, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 6, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 5; i <= 60; i+=12) {
            final long start = new DateTime(2015, 6, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 7, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 6; i <= 60; i+=12) {
            final long start = new DateTime(2015, 7, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 8, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 7; i <= 60; i+=12) {
            final long start = new DateTime(2015, 8, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 9, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 8; i <= 60; i+=12) {
            final long start = new DateTime(2015, 9, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 10, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 9; i <= 60; i+=12) {
            final long start = new DateTime(2015, 10, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 11, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 10; i <= 60; i+=12) {
            final long start = new DateTime(2015, 11, 1, 0, 0).getMillis();
            final long end = new DateTime(2015, 12, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 11; i <= 60; i+=12) {
            final long start = new DateTime(2015, 12, 1, 0, 0).getMillis();
            final long end = new DateTime(2016, 1, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 12; i <= 60; i+=12) {
            final long start = new DateTime(2016, 1, 1, 0, 0).getMillis();
            final long end = new DateTime(2016, 2, 1, 0, 0).getMillis();
            Assert.assertEquals(StringGroupKey.fromTimeRange(FORMATTER, start, end, formatter), yearMonthGroupKey.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(60, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final YearMonthGroupKeySet yearMonthGroupKey = create();
        Assert.assertFalse(yearMonthGroupKey.isPresent(0));
        for (int i = 1; i <= 60; i++) {
            Assert.assertTrue(yearMonthGroupKey.isPresent(i));
        }
        Assert.assertFalse(yearMonthGroupKey.isPresent(61));
    }
}