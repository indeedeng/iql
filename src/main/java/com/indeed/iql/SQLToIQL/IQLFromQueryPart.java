package com.indeed.iql.SQLToIQL;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class IQLFromQueryPart implements IQLQueryPart {

    private static final String COMMAND_NAME = "from";
    private final ImmutableList<IQLFromQueryStatement> iqlFromQueryStatements;

    public IQLFromQueryPart(final ImmutableList<IQLFromQueryStatement> iqlFromQueryStatements) {
        this.iqlFromQueryStatements = iqlFromQueryStatements;
    }

    @Override
    public String toString() {

        return COMMAND_NAME + " " + Joiner.on(", ").join(iqlFromQueryStatements);
    }
}
