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
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * This class allocates nothing for any non-constructor method calls.
 */
@EqualsAndHashCode
@ToString
public class SessionNameGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final List<StringGroupKey> groupKeys;

    public SessionNameGroupKeySet(GroupKeySet previous, List<String> sessionNames, final Formatter formatter) {
        this.previous = previous;
        this.groupKeys = new ArrayList<>(sessionNames.size());
        for (final String sessionName : sessionNames) {
            groupKeys.add(StringGroupKey.fromTerm(sessionName, formatter));
        }
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / groupKeys.size();
    }

    @Override
    public GroupKey groupKey(int group) {
        final int sessionOffset = (group - 1) % groupKeys.size();
        return groupKeys.get(sessionOffset);
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * groupKeys.size();
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
