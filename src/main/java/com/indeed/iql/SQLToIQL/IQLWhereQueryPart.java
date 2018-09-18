package com.indeed.iql.SQLToIQL;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class IQLWhereQueryPart implements IQLQueryPart {

    private static final String COMMAND_NAME = "where";
    private final ImmutableList<IQLWhereQueryStatement> iqlWhereQueryStatements;

    public IQLWhereQueryPart(final ImmutableList<IQLWhereQueryStatement> iqlWhereQueryStatements) {
        this.iqlWhereQueryStatements = iqlWhereQueryStatements;
    }

    @Override
    public String toString() {
        if(iqlWhereQueryStatements.size()!=0){
            return COMMAND_NAME + " " + Joiner.on(" AND ").join(iqlWhereQueryStatements);
        }
        else {
            return "";
        }
    }
}
