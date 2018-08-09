package com.indeed.iql.SQLToIQL;

import org.junit.Assert;
import org.junit.Test;


public class IQLWhereQueryPartTest {

    @Test
    public void noWherePart() {
        final String result="";
        RawSQLQueryInfoHandler rawSQLQueryInfoHandler = new RawSQLQueryInfoHandler();

        Assert.assertEquals(result,rawSQLQueryInfoHandler.getWherePart().toString());
    }


}
