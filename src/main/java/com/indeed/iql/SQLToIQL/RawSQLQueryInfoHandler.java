package com.indeed.iql.SQLToIQL;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;

public class RawSQLQueryInfoHandler {

    public final HashMap<String, IQLFromQueryStatement.Builder> fromQueryStatementBuilders = new HashMap<>();
    public final ImmutableList.Builder<IQLWhereQueryStatement> whereQueryStatements = new ImmutableList.Builder<>();
    public final HashMap<String, String> aliasToIdentifier = new HashMap<>();
    public final ImmutableList.Builder<IQLGroupByQueryStatement> groupByStatement = new ImmutableList.Builder<>();


    public IQLFromQueryPart getFromPart() {
        final ImmutableList.Builder<IQLFromQueryStatement> queryStatementBuilder = new ImmutableList.Builder<>();
        for (final IQLFromQueryStatement.Builder builder :  this.fromQueryStatementBuilders.values()) {
            queryStatementBuilder.add(builder.build());
        }
        if(queryStatementBuilder.build().size()>8){
            throw new TableNumberExceedMax();
        }
        return new IQLFromQueryPart(queryStatementBuilder.build());
    }

    public IQLWhereQueryPart getWherePart() {

        return new IQLWhereQueryPart(whereQueryStatements.build());

    }

    public IQLGroupByQueryPart getGroupByPart(){
        return new IQLGroupByQueryPart(groupByStatement.build());
    }

    public static class TableNumberExceedMax extends RuntimeException {
        public TableNumberExceedMax() {
            super("SQL query table number exceed max number(8)");
        }
    }

}
