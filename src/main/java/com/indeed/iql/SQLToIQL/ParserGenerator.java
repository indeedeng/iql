package com.indeed.iql.SQLToIQL;


public interface ParserGenerator {

    public IQLQuery parse(final String sqlInput);

}
