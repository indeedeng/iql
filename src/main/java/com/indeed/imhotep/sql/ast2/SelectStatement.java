/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.sql.ast2;

import com.indeed.imhotep.sql.ast.Expression;

import java.io.Serializable;
import java.util.List;

/**
 * @author vladimir
 */

public class SelectStatement extends IQLStatement implements Serializable {
    public final SelectClause select;
    public final FromClause from;
    public final WhereClause where;
    public final GroupByClause groupBy;
    public final int limit;

    public SelectStatement(SelectClause select, FromClause from, WhereClause where, GroupByClause groupBy, int limit) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.limit = limit;
    }

    // convenience constructor that constructs the clauses for you
    public SelectStatement(
            List<Expression> projections, FromClause from, Expression where,
            List<Expression> groupBy, int limit) {
        this.select = new SelectClause(projections);
        this.from = from;
        this.where = new WhereClause(where);
        this.groupBy = new GroupByClause(groupBy);
        this.limit = limit;
    }

    // limit default constructor
    public SelectStatement(
            List<Expression> projections, FromClause from, Expression where,
            List<Expression> groupBy) {
        this(projections, from, where, groupBy, Integer.MAX_VALUE);
    }

    /**
     * Returns a string representation that can be used to compare with other SelectStatements.
     * limit value is ignored as it changes presentation of results only.
     */
    public String toHashKeyString() {
        final String fromStr = from != null ? from.toString() : "null";
        final String groupByStr = groupBy != null ? groupBy.toString() : "null";
        final String selectStr = select != null ? select.toString() : "null";
        final String whereStr = where != null ? where.toString() : "null";
        return "SelectStatement {from=" + fromStr  +
                ", groupBy=" + groupByStr +
                ", select=" + selectStr +
                ", where=" + whereStr + "}";
    }
}
