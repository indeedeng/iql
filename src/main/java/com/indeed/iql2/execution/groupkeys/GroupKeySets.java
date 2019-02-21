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

package com.indeed.iql2.execution.groupkeys;

import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

public class GroupKeySets {
    private GroupKeySets() {
    }

    public static void appendTo(final StringBuilder sb, final GroupKeySet groupKeySet, final int group, final char separator) {
        final GroupKeySet previous = groupKeySet.previous();
        if (previous != null) {
            appendTo(sb, previous, groupKeySet.parentGroup(group), separator);
        }
        groupKeySet.groupKey(group).appendWithSeparator(sb, separator);
    }
}
