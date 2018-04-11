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
 package com.indeed.imhotep.iql;

import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;

/**
 * @author jplaisance
 */
public final class GroupingFTGSCallbackNoExplode extends EZImhotepSession.FTGSIteratingCallback<GroupStats> {
    private final List<StatReference> statRefs;
    private final Int2ObjectMap<GroupKey> groupKeys;

    public GroupingFTGSCallbackNoExplode(int numStats, List<StatReference> statRefs, Int2ObjectMap<GroupKey> groupKeys) {
        super(numStats);
        this.statRefs = statRefs;
        this.groupKeys = groupKeys;
    }

    public GroupStats intTermGroup(final String field, final long term, final int group) {
        return getStats(group, term);
    }

    public GroupStats stringTermGroup(final String field, final String term, final int group) {
        return getStats(group, term);
    }

    private GroupStats getStats(int group, Object term) {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            stats[i] = getStat(statRefs.get(i));
        }
        return new GroupStats(groupKeys.get(group).add(term), stats);
    }
}
