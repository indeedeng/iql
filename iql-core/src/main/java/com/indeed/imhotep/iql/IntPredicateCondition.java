package com.indeed.imhotep.iql;

import com.google.common.base.Predicate;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class IntPredicateCondition implements Condition {
    private static final Logger log = Logger.getLogger(IntPredicateCondition.class);

    private final Field.IntField intField;
    private final Predicate<Long> predicate;
    private final boolean negation;

    public IntPredicateCondition(final Field.IntField intField, final Predicate<Long> predicate, final boolean negation) {
        this.intField = intField;
        this.predicate = predicate;
        this.negation = negation;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterNegation(intField, predicate);
        } else {
            session.filter(intField, predicate);
        }
    }
}
