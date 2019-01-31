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
import com.indeed.iql2.execution.groupkeys.InitialGroupKey;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class DumbGroupKeySet implements GroupKeySet {
    public static final DumbGroupKeySet INITIAL_GROUP_KEY_SET = new DumbGroupKeySet(null, new int[]{-1, -1}, Arrays.<GroupKey>asList(null, InitialGroupKey.INSTANCE));
    public final GroupKeySet previous;
    public final int[] groupParents;
    public final List<GroupKey> groupKeys;

    public static DumbGroupKeySet empty() {
        return INITIAL_GROUP_KEY_SET;
    }

    public static DumbGroupKeySet create(GroupKeySet previous, int[] groupParents, List<GroupKey> groupKeys) {
        return new DumbGroupKeySet(previous, groupParents, groupKeys);
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return groupParents[group];
    }

    @Override
    public GroupKey groupKey(int group) {
        return groupKeys.get(group);
    }

    @Override
    public int numGroups() {
        return groupKeys.size() - 1;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group < groupParents.length && groupKeys.get(group) != null && (previous == null || previous.isPresent(parentGroup(group)));
    }
}
