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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.SingleStatReference;
import com.indeed.iql1.ez.Stats;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class MetricCondition implements Condition {
    private static final Logger log = Logger.getLogger(MetricCondition.class);
    private final Stats.Stat stat;
    private final long min;
    private final long max;
    private final boolean negation;

    public MetricCondition(Stats.Stat stat, long min, long max, boolean negation) {
        this.stat = stat;
        this.min = min;
        this.max = max;
        this.negation = negation;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final SingleStatReference statReference = session.pushSingleStat(stat);
        try {
            if(negation) {
                session.filterNegation(statReference, min, max);
            } else {
                session.filter(statReference, min, max);
            }
        } finally {
            session.popStat();
        }
    }
}
