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

import com.google.common.collect.Lists;
import com.indeed.iql2.execution.commands.StringRegroupFieldIn;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

// TODO: Test WITH DEFAULT
public class TestStringFieldInGroupKeySet {
    private static StringRegroupFieldIn.StringFieldInGroupKeySet create() {
        final DumbGroupKeySet previous = DumbGroupKeySet.create(DumbGroupKeySet.create(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new StringRegroupFieldIn.StringFieldInGroupKeySet(previous, Lists.newArrayList("a", "c", "Zzz"), false);
    }

    @Test
    public void testParentGroup() throws Exception {
        final StringRegroupFieldIn.StringFieldInGroupKeySet keySet = create();
        for (int i = 1; i <= 3; i++) {
            Assert.assertEquals(1, keySet.parentGroup(i));
        }
        for (int i = 4; i <= 6; i++) {
            Assert.assertEquals(2, keySet.parentGroup(i));
        }
        for (int i = 7; i <= 9; i++) {
            Assert.assertEquals(3, keySet.parentGroup(i));
        }
        for (int i = 10; i <= 12; i++) {
            Assert.assertEquals(4, keySet.parentGroup(i));
        }
        for (int i = 13; i <= 15; i++) {
            Assert.assertEquals(5, keySet.parentGroup(i));
        }
    }

    @Test
    public void testGroupKey() throws Exception {
        final StringRegroupFieldIn.StringFieldInGroupKeySet keySet = create();
        for (int i = 1; i <= 15; i += 3) {
            Assert.assertEquals(new StringGroupKey("a"), keySet.groupKey(i));
        }
        for (int i = 2; i <= 15; i += 3) {
            Assert.assertEquals(new StringGroupKey("c"), keySet.groupKey(i));
        }
        for (int i = 3; i <= 15; i += 3) {
            Assert.assertEquals(new StringGroupKey("Zzz"), keySet.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(15, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final StringRegroupFieldIn.StringFieldInGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        for (int i = 1; i <= 15; i++) {
            Assert.assertTrue(keySet.isPresent(i));
        }
        Assert.assertFalse(keySet.isPresent(16));
    }
}