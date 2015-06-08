package com.indeed.jql.language.commands;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;

import java.util.Set;

public class GetGroupDistincts implements Command {
    public final Set<String> scope;
    public final String field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistincts(Set<String> scope, String field, Optional<AggregateFilter> filter, int windowSize) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    @Override
    public String toString() {
        return "GetGroupDistincts{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", filter=" + filter +
                ", windowSize=" + windowSize +
                '}';
    }
}
