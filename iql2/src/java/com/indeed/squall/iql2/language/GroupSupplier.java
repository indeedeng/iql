package com.indeed.squall.iql2.language;

public interface GroupSupplier {
    int acquire();
    void release(int group);
}
