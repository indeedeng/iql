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
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.HighGutterGroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.LowGutterGroupKey;
import com.indeed.iql2.execution.groupkeys.RangeGroupKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

// TODO: test WITH DEFAULT
// TODO: test no gutters
// TODO: test interval=1
public class TestMetricRangeGroupKeySet {

    public static final Formatter FORMATTER = Formatter.TSV;

    private static MetricRangeGroupKeySet create() {
        final DumbGroupKeySet previous = DumbGroupKeySet.create(DumbGroupKeySet.empty(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new MetricRangeGroupKeySet(previous, 7, false, 0, 2, false, false, FORMATTER);
    }

    @Test
    public void testParentGroup() throws Exception {
        final MetricRangeGroupKeySet keySet = create();
        for (int i = 1; i <= 7; i++) {
            Assert.assertEquals(1, keySet.parentGroup(i));
        }
        for (int i = 8; i <= 14; i++) {
            Assert.assertEquals(2, keySet.parentGroup(i));
        }
        for (int i = 15; i <= 21; i++) {
            Assert.assertEquals(3, keySet.parentGroup(i));
        }
        for (int i = 22; i <= 28; i++) {
            Assert.assertEquals(4, keySet.parentGroup(i));
        }
        for (int i = 29; i <= 35; i++) {
            Assert.assertEquals(5, keySet.parentGroup(i));
        }
    }

    @Test
    public void testGroupKey() throws Exception {
        final MetricRangeGroupKeySet keySet = create();
        for (int i = 1; i <= 35; i += 7) {
            Assert.assertEquals(new RangeGroupKey(0, 2, FORMATTER), keySet.groupKey(i));
        }
        for (int i = 2; i <= 35; i += 7) {
            Assert.assertEquals(new RangeGroupKey(2, 4, FORMATTER), keySet.groupKey(i));
        }
        for (int i = 3; i <= 35; i += 7) {
            Assert.assertEquals(new RangeGroupKey(4, 6, FORMATTER), keySet.groupKey(i));
        }
        for (int i = 4; i <= 35; i += 7) {
            Assert.assertEquals(new RangeGroupKey(6, 8, FORMATTER), keySet.groupKey(i));
        }
        for (int i = 5; i <= 35; i += 7) {
            Assert.assertEquals(new RangeGroupKey(8, 10, FORMATTER), keySet.groupKey(i));
        }
        for (int i = 6; i <= 35; i += 7) {
            Assert.assertEquals(new LowGutterGroupKey(0), keySet.groupKey(i));
        }
        for (int i = 7; i <= 35; i += 7) {
            Assert.assertEquals(new HighGutterGroupKey(10), keySet.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(35, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final MetricRangeGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        for (int i = 1; i <= 35; i++) {
            Assert.assertTrue(keySet.isPresent(i));
        }
        Assert.assertFalse(keySet.isPresent(36));
    }
}