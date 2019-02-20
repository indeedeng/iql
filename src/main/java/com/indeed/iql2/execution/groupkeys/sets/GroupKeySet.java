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

import java.util.Arrays;

public interface GroupKeySet {
    GroupKeySet previous();
    int parentGroup(int group);
    GroupKey groupKey(int group);
    int numGroups();
    boolean isPresent(int group);

    default int[] getParents() {
        final int[] parents = new int[numGroups() + 1];
        Arrays.setAll(parents, this::parentGroup);
        return parents;
    }
}
