package com.indeed.squall.iql2.server.web.servlets.query;

import com.indeed.squall.iql2.execution.compat.Consumer;

public class CountingConsumer<T> implements Consumer<T> {
    private final Consumer<T> inner;
    private int count = 0;

    public CountingConsumer(Consumer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void accept(T t) {
        count += 1;
        inner.accept(t);
    }

    public int getCount() {
        return count;
    }
}
