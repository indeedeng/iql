package com.indeed.imhotep.iql;

import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class QueryCondition implements Condition {
    private static final Logger log = Logger.getLogger(QueryCondition.class);

    private final Query query;
    private final boolean negation;

    public QueryCondition(final Query query, boolean negation) {
        this.query = query;
        this.negation = negation;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterNegation(query);
        } else {
            session.filter(query);
        }
    }
}
