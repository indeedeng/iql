package com.indeed.squall.iql2.execution;

import com.indeed.imhotep.api.ImhotepSession;

public interface WrappingImhotepSession {
    ImhotepSession wrapped();
}
