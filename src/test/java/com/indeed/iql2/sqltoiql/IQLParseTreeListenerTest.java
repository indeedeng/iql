/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.sqltoiql;


import com.indeed.iql2.sqltoiql.antlr.SQLiteParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IQLParseTreeListenerTest {

    @Test
    public void doNothingWithChildrenCountNot3() {
        SQLiteParser.ExprContext exprContext = mock(SQLiteParser.ExprContext.class);
        when(exprContext.getChildCount()).thenReturn(5);

        final IQLParseTreeListener testIQLParseTreeListener = new IQLParseTreeListener();
        testIQLParseTreeListener.exitExpr(exprContext);

        Assert.assertEquals(0, testIQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, testIQLParseTreeListener.handler.whereQueryStatements.build().size());


    }
    @Test
    public void doNothingWithAND(){
        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        ParseTree parseTreeAtChild1= mock(ParseTree.class);

        when(exprContext.getChild(1)).thenReturn(parseTreeAtChild1);
        when(parseTreeAtChild1.getText()).thenReturn("AND");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.exitExpr(exprContext);

        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
    }

    @Test
    public void doNothingWithOR(){
        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        ParseTree parseTreeAtChild1= mock(ParseTree.class);

        when(exprContext.getChild(1)).thenReturn(parseTreeAtChild1);
        when(parseTreeAtChild1.getText()).thenReturn("OR");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.exitExpr(exprContext);
        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
    }

    @Test
    public void doNothingWithDot(){
        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        ParseTree parseTreeAtChild1= mock(ParseTree.class);

        when(exprContext.getChild(1)).thenReturn(parseTreeAtChild1);
        when(parseTreeAtChild1.getText()).thenReturn(".");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.exitExpr(exprContext);
        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
    }

    @Test
    public void addWhereExpression(){
        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        ParseTree parseTreeChild1=mock(ParseTree.class);
        ParseTree parseTreeChild0=mock(ParseTree.class);
        ParseTree parseTreeChild2=mock(ParseTree.class);
        when(exprContext.getChildCount()).thenReturn(3);
        when(exprContext.getChild(1)).thenReturn(parseTreeChild1);
        when(exprContext.getChild(0)).thenReturn(parseTreeChild0);
        when(exprContext.getChild(2)).thenReturn(parseTreeChild2);
        when(parseTreeChild1.getText()).thenReturn("=");
        when(parseTreeChild0.getText()).thenReturn("a");
        when(parseTreeChild2.getText()).thenReturn("mobjob");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.exitExpr(exprContext);

        Assert.assertEquals(1, IQLParseTreeListener.handler.whereQueryStatements.build().size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());

    }


    @Test
    public void setAlias(){
        SQLiteParser.Table_or_subqueryContext table_or_subqueryContext= mock(SQLiteParser.Table_or_subqueryContext.class);
        ParseTree alias= mock(ParseTree.class);
        ParseTree identifier= mock(ParseTree.class);
        when(table_or_subqueryContext.getChildCount()).thenReturn(3);
        when(table_or_subqueryContext.getChild(2)).thenReturn(alias);
        when(table_or_subqueryContext.getChild(0)).thenReturn(identifier);
        when(identifier.getText()).thenReturn("a");
        when(alias.getText()).thenReturn("b");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.exitTable_or_subquery(table_or_subqueryContext);

        Assert.assertEquals(1, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
        Assert.assertEquals(Optional.of("b"),IQLParseTreeListener.handler.fromQueryStatementBuilders.get("a").getAlias() );
    }

    @Test
    public void setNoAlias(){
        SQLiteParser.Table_or_subqueryContext table_or_subqueryContext= mock(SQLiteParser.Table_or_subqueryContext.class);
        ParseTree identifier= mock(ParseTree.class);
        when(table_or_subqueryContext.getChild(0)).thenReturn(identifier);
        when(identifier.getText()).thenReturn("a");
        when(table_or_subqueryContext.getChildCount()).thenReturn(5);

        final IQLParseTreeListener listener = new IQLParseTreeListener();
        listener.exitTable_or_subquery(table_or_subqueryContext);

        Assert.assertEquals(1, listener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, listener.handler.whereQueryStatements.build().size());
        Assert.assertTrue(listener.handler.fromQueryStatementBuilders.containsKey("a"));
        Assert.assertEquals(Optional.empty(), listener.handler.fromQueryStatementBuilders.get("a").getAlias());

    }


    @Test
    public void addStartTimeExpression(){

        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        when(exprContext.getChildCount()).thenReturn(3);
        ParseTree operator = mock(ParseTree.class);
        when(operator.getText()).thenReturn(">");
        when(exprContext.getChild(1)).thenReturn(operator);
        ParseTree time= mock(ParseTree.class);
        when(exprContext.getChild(2)).thenReturn(time);
        when(time.getText()).thenReturn("\"2018-01-06 00:00:00\"");
        ParseTree midexpr= mock(ParseTree.class);
        when(exprContext.getChild(0)).thenReturn(midexpr);
        ParseTree identification = mock(ParseTree.class);
        when(midexpr.getChild(0)).thenReturn(identification);
        when(identification.getText()).thenReturn("a");

        final IQLParseTreeListener Listener= new IQLParseTreeListener();
        Listener.handler.fromQueryStatementBuilders.put("a",new IQLFromQueryStatement.Builder("a"));
        Listener.exitExpr(exprContext);

        Assert.assertEquals(Optional.of("2018-01-06T00:00").toString(), Listener.handler.fromQueryStatementBuilders.get("a").getStartTime().toString());
        Assert.assertEquals(Optional.empty(), Listener.handler.fromQueryStatementBuilders.get("a").getEndTime());

    }

    @Test
    public void addEndTimeExpression(){

        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        when(exprContext.getChildCount()).thenReturn(3);
        ParseTree operator = mock(ParseTree.class);
        when(operator.getText()).thenReturn(">");
        when(exprContext.getChild(1)).thenReturn(operator);
        ParseTree time= mock(ParseTree.class);
        when(exprContext.getChild(2)).thenReturn(time);
        when(time.getText()).thenReturn("\"2018-02-06 00:00:00\"");
        ParseTree midexpr= mock(ParseTree.class);
        when(exprContext.getChild(0)).thenReturn(midexpr);
        ParseTree identification = mock(ParseTree.class);
        when(midexpr.getChild(0)).thenReturn(identification);
        when(identification.getText()).thenReturn("a");

        final IQLParseTreeListener Listener= new IQLParseTreeListener();
        IQLFromQueryStatement.Builder builder= new IQLFromQueryStatement.Builder("a");
        Listener.handler.fromQueryStatementBuilders.put("a",builder.addTime(LocalDateTime.parse("2018-01-06T00:00")));
        Listener.exitExpr(exprContext);

        Assert.assertEquals(Optional.of("2018-01-06T00:00").toString(), Listener.handler.fromQueryStatementBuilders.get("a").getStartTime().toString());
        Assert.assertEquals(Optional.of("2018-02-06T00:00").toString(), Listener.handler.fromQueryStatementBuilders.get("a").getEndTime().toString());

    }

    @Test(expected = IQLParseTreeListener.IQLParseTreeListenerException.class)
    public void addNoIDTimeExpression(){

        SQLiteParser.ExprContext exprContext= mock(SQLiteParser.ExprContext.class);
        when(exprContext.getChildCount()).thenReturn(3);
        ParseTree operator = mock(ParseTree.class);
        when(operator.getText()).thenReturn(">");
        when(exprContext.getChild(1)).thenReturn(operator);
        ParseTree time= mock(ParseTree.class);
        when(exprContext.getChild(2)).thenReturn(time);
        when(time.getText()).thenReturn("\"2018-02-06 00:00:00\"");
        ParseTree midexpr= mock(ParseTree.class);
        when(exprContext.getChild(0)).thenReturn(midexpr);
        ParseTree identification = mock(ParseTree.class);
        when(midexpr.getChild(0)).thenReturn(identification);
        when(identification.getText()).thenReturn("b");

        final IQLParseTreeListener Listener= new IQLParseTreeListener();
        IQLFromQueryStatement.Builder builder= new IQLFromQueryStatement.Builder("a");
        Listener.handler.fromQueryStatementBuilders.put("a",builder.addTime(LocalDateTime.parse("2018-01-06T00:00")));
        Listener.exitExpr(exprContext);


    }

    @Test
    public void addGroupByExpression(){

        SQLiteParser.Group_byContext group_byContext= mock(SQLiteParser.Group_byContext.class);

        ParseTree parseTreeChild0=mock(ParseTree.class);
        ParseTree parseTreeChild1=mock(ParseTree.class);
        ParseTree parseTreeChild2=mock(ParseTree.class);
        when(group_byContext.getChild(0)).thenReturn(parseTreeChild0);
        when(group_byContext.getChild(1)).thenReturn(parseTreeChild1);
        when(group_byContext.getChild(2)).thenReturn(parseTreeChild2);

        when(group_byContext.getChildCount()).thenReturn(3);
        when(parseTreeChild0.getText()).thenReturn("column1");
        when(parseTreeChild1.getText()).thenReturn(",");
        when(parseTreeChild2.getText()).thenReturn("column2");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.enterGroup_by(group_byContext);

        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(2, IQLParseTreeListener.handler.groupByStatement.build().size());

    }

    @Test
    public void noGroupByExpressionAdded(){
        SQLiteParser.Select_coreContext enterSelect_core= mock(SQLiteParser.Select_coreContext.class);
        ParseTree parseTreeChild0=mock(ParseTree.class);
        ParseTree parseTreeChild1=mock(ParseTree.class);
        ParseTree parseTreeChild2=mock(ParseTree.class);
        when(enterSelect_core.getChild(0)).thenReturn(parseTreeChild0);
        when(enterSelect_core.getChild(1)).thenReturn(parseTreeChild1);
        when(enterSelect_core.getChild(2)).thenReturn(parseTreeChild2);
        when(enterSelect_core.getChildCount()).thenReturn(3);
        when(parseTreeChild0.getText()).thenReturn("select");
        when(parseTreeChild1.getText()).thenReturn("from");
        when(parseTreeChild2.getText()).thenReturn("where");

        final IQLParseTreeListener IQLParseTreeListener = new IQLParseTreeListener();
        IQLParseTreeListener.enterSelect_core(enterSelect_core);

        Assert.assertEquals(0, IQLParseTreeListener.handler.whereQueryStatements.build().size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.fromQueryStatementBuilders.size());
        Assert.assertEquals(0, IQLParseTreeListener.handler.groupByStatement.build().size());

    }


}