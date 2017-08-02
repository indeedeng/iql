package com.indeed.squall.iql2.execution.compat;

public interface Consumer<T> {
    void accept(T t);

    class NoOpConsumer<T> implements Consumer<T> {
        @Override
        public void accept(T t) {

        }
    }
}
