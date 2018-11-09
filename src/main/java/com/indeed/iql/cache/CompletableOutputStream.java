package com.indeed.iql.cache;

import java.io.OutputStream;

public abstract class CompletableOutputStream extends OutputStream {
    protected boolean completed = false;

    public void complete() {
        completed = true;
    }
}
