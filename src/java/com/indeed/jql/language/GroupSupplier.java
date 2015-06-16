package com.indeed.jql.language;

public interface GroupSupplier {
    int acquire();
    void release(int group);
}
