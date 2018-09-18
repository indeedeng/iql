package com.indeed.iql.SQLToIQL;


import com.google.common.base.Joiner;

import java.util.Arrays;

public class IQLWhereQueryStatement {

    private final String leftSide;
    private final IQLOperator operator;
    private final String rightSide;

    public IQLWhereQueryStatement(final String leftSide, final IQLOperator operator, final String rightSide) {
        this.leftSide = leftSide;
        this.operator = operator;
        this.rightSide = rightSide;
    }

    @Override
    public String toString() {
        if(leftSide!=null && rightSide!=null && operator!=null)
        return Joiner.on(" ").join(Arrays.asList(leftSide, operator, rightSide));
        else return "";
    }

}

