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

import com.indeed.iql2.execution.TimeUnit;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.iql2.execution.groupkeys.sets.YearMonthGroupKey;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestYearMonthGroupKey {
    private static YearMonthGroupKey create() {
        final DumbGroupKeySet dumbGroupKeySet = DumbGroupKeySet.create(DumbGroupKeySet.create(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new YearMonthGroupKey(dumbGroupKeySet, 12, new DateTime(2015, 2, 1, 0, 0, 0), TimeUnit.MONTH.formatString);
    }

    @Test
    public void testParentGroup() throws Exception {
        final YearMonthGroupKey yearMonthGroupKey = create();
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
        final YearMonthGroupKey yearMonthGroupKey = create();
        for (int i = 1; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("February 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 2; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("March 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 3; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("April 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 4; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("May 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 5; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("June 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 6; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("July 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 7; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("August 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 8; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("September 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 9; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("October 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 10; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("November 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 11; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("December 2015"), yearMonthGroupKey.groupKey(i));
        }
        for (int i = 12; i <= 60; i+=12) {
            Assert.assertEquals(new StringGroupKey("January 2016"), yearMonthGroupKey.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(60, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final YearMonthGroupKey yearMonthGroupKey = create();
        Assert.assertFalse(yearMonthGroupKey.isPresent(0));
        for (int i = 1; i <= 60; i++) {
            Assert.assertTrue(yearMonthGroupKey.isPresent(i));
        }
        Assert.assertFalse(yearMonthGroupKey.isPresent(61));
    }
}