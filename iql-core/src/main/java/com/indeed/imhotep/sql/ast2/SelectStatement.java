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
