package com.indeed.iql.SQLToIQL;

import org.junit.Assert;
import org.junit.Test;


public class IQLOperatorTest {

    @Test(expected = IQLOperator.UnknownOperatorException.class)
    public void testUnknownOperator() {

        Assert.assertEquals(IQLOperator.values(),
                IQLOperator.fromString("!"));

    }

}