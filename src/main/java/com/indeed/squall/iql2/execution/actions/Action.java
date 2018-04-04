package com.indeed.squall.iql2.execution.actions;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

public interface Action {

    void apply(Session session) throws ImhotepOutOfMemoryException;

}
