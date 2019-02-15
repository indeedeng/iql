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

import com.indeed.iql2.execution.commands.ExplodeDayOfWeek;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class DayOfWeekGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;

    public DayOfWeekGroupKeySet(final GroupKeySet previous) {
        this.previous = previous;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / ExplodeDayOfWeek.DAY_KEYS.length;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int dayOfWeek = (group - 1) % ExplodeDayOfWeek.DAY_KEYS.length;
        return ExplodeDayOfWeek.DAY_GROUP_KEYS[dayOfWeek];
    }

    @Override
    public int numGroups() {
        return ExplodeDayOfWeek.DAY_KEYS.length * previous.numGroups();
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
