package com.indeed.squall.iql2.execution;

import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepSession;

public abstract class WrappingImhotepSession implements HasSessionId {
    private final ImhotepSession wrapped;

    protected WrappingImhotepSession(ImhotepSession wrapped) {
        this.wrapped = wrapped;
    }

    public ImhotepSession wrapped() {
        return wrapped;
    }

    @Override
    public String getSessionId() {
        if (wrapped instanceof HasSessionId) {
            return ((HasSessionId) wrapped).getSessionId();
        }
        return null;
    }
}
