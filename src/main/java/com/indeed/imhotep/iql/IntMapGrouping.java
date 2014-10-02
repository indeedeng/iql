package com.indeed.imhotep.iql;

import com.google.common.base.Function;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * @author jplaisance
 */
public final class IntMapGrouping<A> extends Grouping {
    private static final Logger log = Logger.getLogger(IntMapGrouping.class);

    private final Field.IntField intField;
    private final Function<Integer, A> function;

    public IntMapGrouping(final Field.IntField intField, final Function<Integer, A> function) {
        this.intField = intField;
        this.function = function;
    }

    public Map<Integer, GroupKey> regroup(final EZImhotepSession session, final Map<Integer, GroupKey> groupKeys) {
        throw new UnsupportedOperationException();
    }
}
