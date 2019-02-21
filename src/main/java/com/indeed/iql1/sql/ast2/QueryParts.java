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
 package com.indeed.iql1.sql.ast2;

import com.google.common.base.Strings;
import org.codehaus.jparsec.Token;

/**
* @author vladimir
*/
public class QueryParts {
    public String from = "";
    public String where = "";
    public String groupBy = "";
    public String select = "";
    public String limit = "";

    public String error;

    public int fromStart, fromEnd, whereStart, whereEnd, groupByStart, groupByEnd, selectStart, selectEnd;

    public QueryParts() {
    }

    public QueryParts(String from, String where, String groupBy, String select, String limit) {
        this.from = Strings.nullToEmpty(from).trim();
        this.where = Strings.nullToEmpty(where).trim();
        this.groupBy = Strings.nullToEmpty(groupBy).trim();
        this.select = Strings.nullToEmpty(select).trim();
        this.limit = Strings.nullToEmpty(limit).trim();
    }

    public QueryParts(String from, String where, String groupBy, String select) {
        this(from, where, groupBy, select, "");
    }

    public QueryParts(Token from, Token where, Token groupBy, Token select, Token limit) {
        this(tokenAsString(from), tokenAsString(where), tokenAsString(groupBy), tokenAsString(select), tokenAsString(limit));

        if(from != null) {
            fromStart = from.index();
            fromEnd = from.index() + from.length();
        }
        if(select != null) {
            selectStart = select.index();
            selectEnd = select.index() + select.length();
        }
        if(where != null) {
            whereStart = where.index();
            whereEnd = where.index() + where.length();
        }
        if(groupBy != null) {
            groupByStart = groupBy.index();
            groupByEnd = groupBy.index() + groupBy.length();
        }
    }

    private static String tokenAsString(Token token) {
        return token != null ? (String) token.value() : "";
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean linqOrder) {
        if(linqOrder) {
            return "from " + from +
                    (Strings.isNullOrEmpty(where) ? "" : " where " + where) +
                    (Strings.isNullOrEmpty(groupBy) ? "" : " group by " + groupBy) +
                    " select " + select +
                    (Strings.isNullOrEmpty(limit) ? "" : " limit " + limit);
        } else {
            return "select " + select +
                    " from " + from +
                    (Strings.isNullOrEmpty(where) ? "" : " where " + where) +
                    (Strings.isNullOrEmpty(groupBy) ? "" : " group by " + groupBy) +
                    (Strings.isNullOrEmpty(limit) ? "" : " limit " + limit);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryParts)) {
            return false;
        }

        QueryParts that = (QueryParts) o;

        if (!from.equals(that.from)) {
            return false;
        }
        if (!groupBy.equals(that.groupBy)) {
            return false;
        }
        if (!limit.equals(that.limit)) {
            return false;
        }
        if (!select.equals(that.select)) {
            return false;
        }
        if (!where.equals(that.where)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = from.hashCode();
        result = 31 * result + where.hashCode();
        result = 31 * result + groupBy.hashCode();
        result = 31 * result + select.hashCode();
        result = 31 * result + limit.hashCode();
        return result;
    }
}
