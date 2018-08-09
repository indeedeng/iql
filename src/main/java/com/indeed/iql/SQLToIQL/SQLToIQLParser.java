package com.indeed.iql.SQLToIQL;

public class SQLToIQLParser {

    private final ParserGenerator parserGenerator;

    public SQLToIQLParser(ParserGenerator parserGenerator){
        this.parserGenerator=parserGenerator;
    }

    public String parse(final String sqlInput) {
        final IQLQuery query = parserGenerator.parse(sqlInput);
        return query.toString();
    }


}
