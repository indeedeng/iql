package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.StatReference;
import com.indeed.imhotep.ez.Stats;

import java.util.Map;

/**
 * @author jplaisance
 */
public interface Condition {

    public void filter(EZImhotepSession session) throws ImhotepOutOfMemoryException;
}
