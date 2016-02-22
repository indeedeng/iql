package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;
import java.util.Objects;

public class IntTermGroupKey extends GroupKey {
    public final long value;

    public IntTermGroupKey(long value) {
        this.value = value;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(String.valueOf(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntTermGroupKey that = (IntTermGroupKey) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "IntTermGroupKey{" +
                "value=" + value +
                '}';
    }
}
