package com.indeed.squall.iql2.language;

public interface BinOp<T> {
    T apply(T t1, T t2);
}
