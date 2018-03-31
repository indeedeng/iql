package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class DefaultGroupKey extends GroupKey {
    public static final DefaultGroupKey DEFAULT_INSTANCE = new DefaultGroupKey("DEFAULT");

    private final String name;

    private DefaultGroupKey(String name) {
        this.name = name;
    }

    public static DefaultGroupKey create(String defaultGroupName) {
        if (defaultGroupName.equals("DEFAULT")) {
            return DEFAULT_INSTANCE;
        } else {
            return new DefaultGroupKey(defaultGroupName);
        }
    }

    @Override
    public void addToList(List<String> list) {
        list.add(name);
    }

    @Override
    public boolean isDefault() {
        return true;
    }

    @Override
    public String toString() {
        return "DefaultGroupKey{" +
                "name='" + name + '\'' +
                '}';
    }
}
