package com.indeed.iql.SQLToIQL;


public class IQLGroupByQueryStatement {

    private final String columnName;

    public IQLGroupByQueryStatement(final String columnName) {
        this.columnName=columnName;
    }

    @Override
    public String toString() {
         return this.columnName;
    }


}
