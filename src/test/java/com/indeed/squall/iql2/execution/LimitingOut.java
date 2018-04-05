package com.indeed.squall.iql2.execution;

import com.indeed.squall.iql2.execution.compat.Consumer;

/**
 * @author jwolfe
 */

public class LimitingOut<T> implements Consumer<T> {
    private final Consumer<T> out;
    private final int limit;
    private int seen = 0;

    public LimitingOut(final Consumer<T> out, final int limit) {
        this.out = out;
        this.limit = limit;
    }

    @Override
    public void accept(final T s) {
        if (seen < limit) {
            out.accept(s);
            seen += 1;
        }
    }
}
