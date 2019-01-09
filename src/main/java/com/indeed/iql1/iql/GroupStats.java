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
 package com.indeed.iql1.iql;

import com.indeed.iql1.ez.GroupKey;

import java.util.Comparator;

/**
* @author jplaisance
*/
public final class GroupStats {
    final GroupKey groupKey;
    final double[] stats;

    public GroupStats(final GroupKey groupKey, final double[] stats) {
        this.groupKey = groupKey;
        this.stats = stats;
    }

    public GroupKey getGroupKey() {
        return groupKey;
    }

    public double[] getStats() {
        return stats;
    }

    public static final Comparator<GroupStats> GROUP_STATS_COMPARATOR =
            (o1, o2) -> o2.groupKey.getLastInserted().compareTo(o1.groupKey.getLastInserted());

}
