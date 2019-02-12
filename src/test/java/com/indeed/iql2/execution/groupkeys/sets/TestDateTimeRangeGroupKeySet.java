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
import com.indeed.iql2.execution.TimeUnit;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

public class TestDateTimeRangeGroupKeySet {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    private static DateTimeRangeGroupKeySet create() {
        final DumbGroupKeySet previous = DumbGroupKeySet.create(DumbGroupKeySet.empty(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new DateTimeRangeGroupKeySet(previous, new DateTime(2015, 2, 23, 12, 0, 0).getMillis(), TimeUnit.HOUR.millis, 24, 24 * previous.numGroups(), TimeUnit.HOUR.formatString, Formatter.TSV);
    }

    @Test
    public void testParentGroup() throws Exception {
        final DateTimeRangeGroupKeySet keySet = create();
        for (int i = 1; i <= 24; i++) {
            Assert.assertEquals(1, keySet.parentGroup(i));
        }
        for (int i = 25; i <= 48; i++) {
            Assert.assertEquals(2, keySet.parentGroup(i));
        }
        for (int i = 49; i <= 72; i++) {
            Assert.assertEquals(3, keySet.parentGroup(i));
        }
        for (int i = 73; i <= 96; i++) {
            Assert.assertEquals(4, keySet.parentGroup(i));
        }
        for (int i = 97; i <= 120; i++) {
            Assert.assertEquals(5, keySet.parentGroup(i));
        }
    }

    @Test
    public void testGroupKey() throws Exception {
        final String format = TimeUnit.HOUR.formatString;
        final DateTimeRangeGroupKeySet keySet = create();
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(format).withLocale(Locale.US);
        final Formatter escaper = Formatter.TSV;
        for (int i = 1; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 12, 0, 0).getMillis(), new DateTime(2015, 2, 23, 13, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 2; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 13, 0, 0).getMillis(), new DateTime(2015, 2, 23, 14, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 3; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 14, 0, 0).getMillis(), new DateTime(2015, 2, 23, 15, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 4; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 15, 0, 0).getMillis(), new DateTime(2015, 2, 23, 16, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 5; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 16, 0, 0).getMillis(), new DateTime(2015, 2, 23, 17, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 6; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 17, 0, 0).getMillis(), new DateTime(2015, 2, 23, 18, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 7; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 18, 0, 0).getMillis(), new DateTime(2015, 2, 23, 19, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 8; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 19, 0, 0).getMillis(), new DateTime(2015, 2, 23, 20, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 9; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 20, 0, 0).getMillis(), new DateTime(2015, 2, 23, 21, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 10; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 21, 0, 0).getMillis(), new DateTime(2015, 2, 23, 22, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 11; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 22, 0, 0).getMillis(), new DateTime(2015, 2, 23, 23, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 12; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 23, 23, 0, 0).getMillis(), new DateTime(2015, 2, 24, 0, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 13; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 0, 0, 0).getMillis(), new DateTime(2015, 2, 24, 1, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 14; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 1, 0, 0).getMillis(), new DateTime(2015, 2, 24, 2, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 15; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 2, 0, 0).getMillis(), new DateTime(2015, 2, 24, 3, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 16; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 3, 0, 0).getMillis(), new DateTime(2015, 2, 24, 4, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 17; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 4, 0, 0).getMillis(), new DateTime(2015, 2, 24, 5, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 18; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 5, 0, 0).getMillis(), new DateTime(2015, 2, 24, 6, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 19; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 6, 0, 0).getMillis(), new DateTime(2015, 2, 24, 7, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 20; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 7, 0, 0).getMillis(), new DateTime(2015, 2, 24, 8, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 21; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 8, 0, 0).getMillis(), new DateTime(2015, 2, 24, 9, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 22; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 9, 0, 0).getMillis(), new DateTime(2015, 2, 24, 10, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 23; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 10, 0, 0).getMillis(), new DateTime(2015, 2, 24, 11, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
        for (int i = 24; i <= 120; i += 24) {
            Assert.assertEquals(StringGroupKey.fromTimeRange(formatter, new DateTime(2015, 2, 24, 11, 0, 0).getMillis(), new DateTime(2015, 2, 24, 12, 0, 0).getMillis(), escaper), keySet.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(120, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final DateTimeRangeGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        for (int i = 1; i <= 120; i++) {
            Assert.assertTrue(keySet.isPresent(i));
        }
        Assert.assertFalse(keySet.isPresent(121));
    }
}