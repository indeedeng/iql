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

import com.indeed.squall.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;

public class RandomGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numGroups;

    public RandomGroupKeySet(GroupKeySet previous, int numGroups) {
        this.previous = previous;
        this.numGroups = numGroups;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1;
    }

    @Override
    public GroupKey groupKey(int group) {
        if (group == 1) {
            return DefaultGroupKey.create("No term");
        }
        return new IntTermGroupKey(group - 1);
    }

    @Override
    public int numGroups() {
        return numGroups;
    }

    @Override
    public boolean isPresent(int group) {
        return group <= numGroups;
    }
}
