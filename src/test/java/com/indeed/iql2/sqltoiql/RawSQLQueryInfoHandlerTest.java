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

import org.junit.Test;

import java.time.LocalDateTime;

public class RawSQLQueryInfoHandlerTest {

    @Test(expected = RawSQLQueryInfoHandler.TableNumberExceedMax.class)
    public void tableNumberExceedMaxNumber() {

        final RawSQLQueryInfoHandler rawSQLQueryInfoHandler= new RawSQLQueryInfoHandler();

        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("1",new IQLFromQueryStatement.Builder("1").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("2",new IQLFromQueryStatement.Builder("2").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("3",new IQLFromQueryStatement.Builder("3").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("4",new IQLFromQueryStatement.Builder("4").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("5",new IQLFromQueryStatement.Builder("5").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("6",new IQLFromQueryStatement.Builder("6").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("7",new IQLFromQueryStatement.Builder("7").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("8",new IQLFromQueryStatement.Builder("8").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));
        rawSQLQueryInfoHandler.fromQueryStatementBuilders.put("9",new IQLFromQueryStatement.Builder("9").addTime(LocalDateTime.now()).addTime(LocalDateTime.now()));

        rawSQLQueryInfoHandler.getFromPart();

    }

}