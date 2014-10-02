package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class IntInCondition implements Condition {
    private static final Logger log = Logger.getLogger(IntInCondition.class);

    private final Field.IntField intField;
    private final long[] values;
    private final boolean negation;

    public IntInCondition(Field.IntField intField, boolean negation, long... values) {
        this.intField = intField;
        this.negation = negation;
        this.values = values;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterNegation(intField, values);
        } else {
            session.filter(intField, values);
        }
    }
}
