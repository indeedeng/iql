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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import com.indeed.imhotep.ez.Stats;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */

public class DiffGrouping extends Grouping {
    private final Field field;
    private final Stats.Stat filter1;
    private final Stats.Stat filter2;
    private final int topK;

    public DiffGrouping(Field field, Stats.Stat filter1, Stats.Stat filter2, int topK) {
        this.field = field;
        this.filter1 = filter1;
        this.filter2 = filter2;
        this.topK = topK;
    }

    public Field getField() {
        return field;
    }

    public Stats.Stat getFilter1() {
        return filter1;
    }

    public Stats.Stat getFilter2() {
        return filter2;
    }

    public int getTopK() {
        return topK;
    }

    @Override
    public Int2ObjectMap<GroupKey> regroup(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException();  // This should always be rewritten in the IQLTranslator so that it never gets invoked
    }

    @Override
    public Iterator<GroupStats> getGroupStats(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys, List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException();  // This should always be rewritten in the IQLTranslator so that it never gets invoked
    }
}
