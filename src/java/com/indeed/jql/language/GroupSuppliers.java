package com.indeed.jql.language;

import java.util.Stack;

public class GroupSuppliers {
    public static GroupSupplier newGroupSupplier(final int start) {
        return new GroupSupplier() {
            final Stack<Integer> repushed = new Stack<>();
            int next = start;

            @Override
            public int acquire() {
                if (repushed.isEmpty()) {
                    final int result = next;
                    next += 1;
                    return result;
                } else {
                    return repushed.pop();
                }
            }

            @Override
            public void release(int group) {
                repushed.push(group);
            }
        };
    }
}
