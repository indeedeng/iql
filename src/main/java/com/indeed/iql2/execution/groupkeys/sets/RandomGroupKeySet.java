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
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;

public class RandomGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numGroups;
    private final Formatter formatter;

    public RandomGroupKeySet(GroupKeySet previous, int numGroups, final Formatter formatter) {
        this.previous = previous;
        this.numGroups = numGroups;
        this.formatter = formatter;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / numGroups;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int innerGroup = (group - 1) % numGroups;
        if (innerGroup == 0) {
            return DefaultGroupKey.create("No term", formatter);
        }
        return new IntTermGroupKey(innerGroup);
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numGroups;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
