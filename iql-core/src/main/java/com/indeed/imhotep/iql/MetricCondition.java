package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.SingleStatReference;
import com.indeed.imhotep.ez.StatReference;
import com.indeed.imhotep.ez.Stats;
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
        final SingleStatReference statReference = session.pushStat(stat);
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
