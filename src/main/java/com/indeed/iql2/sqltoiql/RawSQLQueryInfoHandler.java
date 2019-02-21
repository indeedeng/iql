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

import com.google.common.collect.ImmutableList;

import java.util.HashMap;

public class RawSQLQueryInfoHandler {

    public final HashMap<String, IQLFromQueryStatement.Builder> fromQueryStatementBuilders = new HashMap<>();
    public final ImmutableList.Builder<IQLWhereQueryStatement> whereQueryStatements = new ImmutableList.Builder<>();
    public final HashMap<String, String> aliasToIdentifier = new HashMap<>();
    public final ImmutableList.Builder<IQLGroupByQueryStatement> groupByStatement = new ImmutableList.Builder<>();


    public IQLFromQueryPart getFromPart() {
        final ImmutableList.Builder<IQLFromQueryStatement> queryStatementBuilder = new ImmutableList.Builder<>();
        for (final IQLFromQueryStatement.Builder builder :  fromQueryStatementBuilders.values()) {
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
