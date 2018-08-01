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

import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.iql2.execution.groupkeys.sets.MaskingGroupKeySet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;

public class TestMaskingGroupKeySet {
    private static MaskingGroupKeySet create() {
        final DumbGroupKeySet wrapped = DumbGroupKeySet.create(DumbGroupKeySet.create(), new int[]{-1, 1, 3, 2, 5, 7}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        final BitSet mask = new BitSet();
        mask.set(1);
        mask.set(2);
        mask.set(4);
        return new MaskingGroupKeySet(wrapped, mask);
    }

    @Test
    public void testParentGroup() throws Exception {
        final MaskingGroupKeySet keySet = create();
        Assert.assertEquals(1, keySet.parentGroup(1));
        Assert.assertEquals(3, keySet.parentGroup(2));
        Assert.assertEquals(2, keySet.parentGroup(3));
        Assert.assertEquals(5, keySet.parentGroup(4));
        Assert.assertEquals(7, keySet.parentGroup(5));
    }

    @Test
    public void testGroupKey() throws Exception {
        final MaskingGroupKeySet keySet = create();
        Assert.assertEquals(new IntTermGroupKey(1), keySet.groupKey(1));
        Assert.assertEquals(new IntTermGroupKey(2), keySet.groupKey(2));
        Assert.assertEquals(new IntTermGroupKey(3), keySet.groupKey(3));
        Assert.assertEquals(new IntTermGroupKey(4), keySet.groupKey(4));
        Assert.assertEquals(new IntTermGroupKey(5), keySet.groupKey(5));
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(5, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final MaskingGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        Assert.assertTrue(keySet.isPresent(1));
        Assert.assertTrue(keySet.isPresent(2));
        Assert.assertFalse(keySet.isPresent(3));
        Assert.assertTrue(keySet.isPresent(4));
        Assert.assertFalse(keySet.isPresent(5));
        Assert.assertFalse(keySet.isPresent(6));
    }
}