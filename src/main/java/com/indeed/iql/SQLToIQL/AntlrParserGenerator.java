package com.indeed.iql.SQLToIQL;


import com.google.common.collect.ImmutableList;
import com.indeed.iql.SQLToIQL.antlr.*;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class AntlrParserGenerator implements ParserGenerator {

    @Override
    public IQLQuery parse(final String sqlInput) {
        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        final ANTLRInputStream antlrInputStream = new ANTLRInputStream(sqlInput);
        final SQLiteLexer sqLiteLexer = new SQLiteLexer(antlrInputStream);
        final CommonTokenStream commonTokenStream = new CommonTokenStream(sqLiteLexer);
        final SQLiteParser sqLiteParser = new SQLiteParser(commonTokenStream);
        ParseTreeWalker.DEFAULT.walk(IQLParseTreeListener, sqLiteParser.parse());
        return new IQLQuery(ImmutableList.of(IQLParseTreeListener.handler.getFromPart(), IQLParseTreeListener.handler.getWherePart(),IQLParseTreeListener.handler.getGroupByPart()));
    }

}
