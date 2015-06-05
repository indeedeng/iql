package com.indeed.jql.language.commands;

import com.indeed.jql.language.AggregateFilter;

public class RegroupIntoLastSiblingWhere implements Command {
    private final AggregateFilter filter;
    private final GroupLookupMergeType mergeType;

    public RegroupIntoLastSiblingWhere(AggregateFilter filter, GroupLookupMergeType mergeType) {
        this.filter = filter;
        this.mergeType = mergeType;
    }
}
