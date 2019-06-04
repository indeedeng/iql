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


import com.indeed.iql2.language.SQLiteParser;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.regex.Pattern;

public class IQLParseTreeListener extends SQLiteBaseLogListener {

    private static final Logger LOG = Logger.getLogger(IQLParseTreeListener.class);

    private static final String HOUR_MINUTE_SECOND = "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]";
    private static final String YEARS = "([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})";
    private static final String LONG_MONTHS_AND_DATE = "((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))";
    private static final String SHORT_MONTHS_AND_DATE = "((0[469]|11)-(0[1-9]|[12][0-9]|30))";
    private static final String FEB_NOT_LUNAR_YEAR = "(02-(0[1-9]|[1][0-9]|2[0-8]))";
    private static final String FEB_LUNAR_YEAR = "((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29)";
    private static final Pattern TIME_EXPRESSION =
            Pattern.compile(
                    "\"((" + YEARS +
                            "-(" + LONG_MONTHS_AND_DATE + "|" + SHORT_MONTHS_AND_DATE + "|" +
                            FEB_NOT_LUNAR_YEAR + "))" + "|" + FEB_LUNAR_YEAR + ") " +
                            HOUR_MINUTE_SECOND + "\"");

    public final RawSQLQueryInfoHandler handler = new RawSQLQueryInfoHandler();


    @Override
    public void enterResult_column(SQLiteParser.Result_columnContext ctx) {
        LOG.debug("Running listener: enterResult_column: " + ctx.getText());
        //TODO in case not select count(*)
    }

    @Override
    public void exitTable_or_subquery(SQLiteParser.Table_or_subqueryContext ctx) {
        LOG.debug("Running listener: enterTable_or_subquery: " + ctx.getText());
        final String identifier = ctx.getChild(0).getText();
        final IQLFromQueryStatement.Builder builder = new IQLFromQueryStatement.Builder(identifier);
        if (ctx.getChildCount() == 3) {
            builder.setAlias(ctx.getChild(2).getText());
            handler.aliasToIdentifier.put(ctx.getChild(2).getText(),ctx.getChild(0).getText());
        }
        handler.fromQueryStatementBuilders.put(identifier, builder);
    }


    @Override
    public void exitExpr(SQLiteParser.ExprContext ctx) {
        LOG.debug("Running listener: exitExpr: " + ctx.getText());

        if (ctx.getChildCount() != 3) {
            return;
        }
        if (Arrays.asList("AND", "OR").contains(ctx.getChild(1).getText().toUpperCase())) {
            return;
        }

        if (ctx.getChild(1).getText().equals(".")) {
            return;
        }

        if (Pattern.matches(TIME_EXPRESSION.pattern(), ctx.getChild(2).getText())) {
            LocalDateTime time = LocalDateTime.parse(ctx.getChild(2).getText().replace("\"", "").replace(" ", "T"));
            final String identifier = ctx.getChild(0).getChild(0).getText();
            if (!handler.fromQueryStatementBuilders.containsKey(identifier) && !handler.aliasToIdentifier.containsKey(identifier)){
                throw new IQLParseTreeListenerException();
            } else {
                if(handler.aliasToIdentifier.containsKey(identifier)) {
                    handler.fromQueryStatementBuilders.get(handler.aliasToIdentifier.get(identifier)).addTime(time);
                }
                else{
                    handler.fromQueryStatementBuilders.get(identifier).addTime(time);
                }
            }

        } else {
            handler.whereQueryStatements.add(
                    new IQLWhereQueryStatement(
                            ctx.getChild(0).getText(),
                            IQLOperator.fromString(ctx.getChild(1).getText()),
                            ctx.getChild(2).getText()));
        }
    }

    @Override
    public void enterGroup_by(SQLiteParser.Group_byContext ctx){
        final int childNumber = ctx.getChildCount();
        for(int i=0;i<childNumber;i++){
            if(!ctx.getChild(i).getText().equals(",")){
                handler.groupByStatement.add(new IQLGroupByQueryStatement(ctx.getChild(i).getText()));
            }
        }

    }


    public static class IQLParseTreeListenerException extends RuntimeException {
        public IQLParseTreeListenerException() {
            super("IQLFromQueryStatement requires an identifier for start and end time");
        }
    }
}