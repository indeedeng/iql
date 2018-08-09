package com.indeed.iql.SQLToIQL;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class IQLGroupByQueryPart implements IQLQueryPart  {
    private static final String COMMAND_NAME = "group by";
    private final ImmutableList<IQLGroupByQueryStatement> iqlGroupByQueryStatements;

    public IQLGroupByQueryPart(final ImmutableList<IQLGroupByQueryStatement> iqlGroupByQueryStatements){
        this.iqlGroupByQueryStatements=iqlGroupByQueryStatements;
    }

    @Override
    public String toString() {
        if(iqlGroupByQueryStatements.size()!=0) {
            return COMMAND_NAME + " " + Joiner.on(", ").join(iqlGroupByQueryStatements);
        }
            return "";
    }
}
