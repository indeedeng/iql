package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.SingleStatReference;
import com.indeed.imhotep.ez.StatReference;
import org.apache.log4j.Logger;

import java.util.Map;

import static com.indeed.imhotep.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class StatRangeGrouping2D extends Grouping {
    private static final Logger log = Logger.getLogger(StatRangeGrouping2D.class);

    private final Stat xStat;
    private final long xMin;
    private final long xMax;
    private final long xIntervalSize;
    private final Stat yStat;
    private final long yMin;
    private final long yMax;
    private final long yIntervalSize;

    public StatRangeGrouping2D(final Stat xStat, final long xMin, final long xMax, final long xIntervalSize, final Stat yStat, final long yMin, final long yMax, final long yIntervalSize) {
        if(xIntervalSize <= 0) {
            throw new IllegalArgumentException("Bucket size has to be positive for stat: " + xStat.toString());
        }
        if(yIntervalSize <= 0) {
            throw new IllegalArgumentException("Bucket size has to be positive for stat: " + yStat.toString());
        }
        this.xStat = xStat;
        this.xMin = xMin;
        this.xMax = xMax;
        this.xIntervalSize = xIntervalSize;
        this.yStat = yStat;
        this.yMin = yMin;
        this.yMax = yMax;
        this.yIntervalSize = yIntervalSize;

        final long expectedBucketCount = ((long)xMax - xMin) / xIntervalSize + ((long)yMax - yMin) / yIntervalSize;
        if(expectedBucketCount > StatRangeGrouping.MAX_BUCKETS || expectedBucketCount < 0) {
            throw new IllegalArgumentException("Requested bucket count for metrics " + xStat.toString() + " & " + yStat.toString() +
                    " is " + expectedBucketCount + " which is over the limit of " + StatRangeGrouping.MAX_BUCKETS);
        }
    }

    public Map<Integer, GroupKey> regroup(final EZImhotepSession session, final Map<Integer, GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        final SingleStatReference xStatRef = session.pushStat(xStat);
        final SingleStatReference yStatRef = session.pushStat(yStat);
        final Map<Integer, GroupKey> ret = session.metricRegroup2D(xStatRef, xMin, xMax, xIntervalSize, yStatRef, yMin, yMax, yIntervalSize);
        session.popStat();
        session.popStat();
        return ret;
    }
}
