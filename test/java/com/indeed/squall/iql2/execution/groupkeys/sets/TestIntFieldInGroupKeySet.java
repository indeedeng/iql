package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.commands.IntRegroupFieldIn;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

// TODO: Test WITH DEFAULT
public class TestIntFieldInGroupKeySet {
    private static IntRegroupFieldIn.IntFieldInGroupKeySet create() {
        final DumbGroupKeySet previous = DumbGroupKeySet.create(DumbGroupKeySet.create(), new int[]{-1, 1, 1, 1, 1, 1}, Arrays.<GroupKey>asList(null, new IntTermGroupKey(1), new IntTermGroupKey(2), new IntTermGroupKey(3), new IntTermGroupKey(4), new IntTermGroupKey(5)));
        return new IntRegroupFieldIn.IntFieldInGroupKeySet(previous, new LongArrayList(new long[]{1, 3, 10, 100}), false);
    }

    @Test
    public void testParentGroup() throws Exception {
        final IntRegroupFieldIn.IntFieldInGroupKeySet keySet = create();
        for (int i = 1; i <= 4; i++) {
            Assert.assertEquals(1, keySet.parentGroup(i));
        }
        for (int i = 5; i <= 8; i++) {
            Assert.assertEquals(2, keySet.parentGroup(i));
        }
        for (int i = 9; i <= 12; i++) {
            Assert.assertEquals(3, keySet.parentGroup(i));
        }
        for (int i = 13; i <= 16; i++) {
            Assert.assertEquals(4, keySet.parentGroup(i));
        }
        for (int i = 17; i <= 20; i++) {
            Assert.assertEquals(5, keySet.parentGroup(i));
        }
    }

    @Test
    public void testGroupKey() throws Exception {
        final IntRegroupFieldIn.IntFieldInGroupKeySet keySet = create();
        for (int i = 1; i <= 20; i += 4) {
            Assert.assertEquals(new IntTermGroupKey(1), keySet.groupKey(i));
        }
        for (int i = 2; i <= 20; i += 4) {
            Assert.assertEquals(new IntTermGroupKey(3), keySet.groupKey(i));
        }
        for (int i = 3; i <= 20; i += 4) {
            Assert.assertEquals(new IntTermGroupKey(10), keySet.groupKey(i));
        }
        for (int i = 4; i <= 20; i += 4) {
            Assert.assertEquals(new IntTermGroupKey(100), keySet.groupKey(i));
        }
    }

    @Test
    public void testNumGroups() throws Exception {
        Assert.assertEquals(20, create().numGroups());
    }

    @Test
    public void testIsPresent() throws Exception {
        final IntRegroupFieldIn.IntFieldInGroupKeySet keySet = create();
        Assert.assertFalse(keySet.isPresent(0));
        for (int i = 1; i <= 20; i++) {
            Assert.assertTrue(keySet.isPresent(i));
        }
        Assert.assertFalse(keySet.isPresent(21));
    }
}