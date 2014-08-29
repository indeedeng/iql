package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;

/**
 * Filters out random terms for the specified field.
 * @author vladimir
 */
public class SampleCondition implements Condition {
    private final Field field;
    private final double p;
    private final String salt;

    /**
     * @param field field to sample by
     * @param p ratio of terms to remove. In the range [0,1]
     * @param salt the salt to use for hashing. Providing a constant salt will lead to a reproducible result.
     */
    public SampleCondition(Field field, double p, String salt, boolean negation) {
        this.field = field;
        this.p = negation ? p : (1-p);
        this.salt = salt;
    }

    @Override
    public void filter(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        session.filterSample(field, p, salt);
    }
}
