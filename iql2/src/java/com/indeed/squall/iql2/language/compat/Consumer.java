package com.indeed.squall.iql2.language.compat;

// TODO: Delete this with JDK8. Or at least share with iql2-execution's Consumer? iql2-common..?
public interface Consumer<T> {
    void accept(T t);
}
