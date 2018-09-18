package com.indeed.iql.SQLToIQL;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class IQLQuery {

    private final ImmutableList<IQLQueryPart> queryParts;

    public IQLQuery(final ImmutableList<IQLQueryPart> queryParts) {
        this.queryParts = queryParts;
    }

    @Override
    public String toString() {
        return Joiner.on(" ").join(queryParts).trim();
    }

}
