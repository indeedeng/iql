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

package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.DayOfWeekGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestDayOfWeekGroupKeySet {
    private static DayOfWeekGroupKeySet create() {
        final DumbGroupKeySet previous = DumbGroupKeySet.create(DumbGroupKeySet.create(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new DayOfWeekGroupKeySet(previous);
    }

    @Test
    public void testParentGroup() throws Exception {
        final DayOfWeekGroupKeySet keySet = create();
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
        final DayOfWeekGroupKeySet keySet = create();
        for (int start = 1; start <= 7; start++) {
            for (int i = start; i <= 35; i += 7) {
                Assert.assertEquals(new DayOfWeekGroupKey(start - 1), keySet.groupKey(i));
            }
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(35, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final DayOfWeekGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        for (int i = 1; i <= 35; i++) {
            Assert.assertTrue(keySet.isPresent(i));
        }
        Assert.assertFalse(keySet.isPresent(36));
    }
}