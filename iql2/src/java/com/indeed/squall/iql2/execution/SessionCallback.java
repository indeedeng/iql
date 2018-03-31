package com.indeed.squall.iql2.execution;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.util.core.TreeTimer;

public interface SessionCallback {
    void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException;
}
