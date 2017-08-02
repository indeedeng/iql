package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;
import java.util.Objects;

public class StringGroupKey extends GroupKey {
    public final String term;

    public StringGroupKey(String term) {
        this.term = term;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(term);
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringGroupKey that = (StringGroupKey) o;
        return Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term);
    }

    @Override
    public String toString() {
        return "StringGroupKey{" +
                "term='" + term + '\'' +
                '}';
    }
}
