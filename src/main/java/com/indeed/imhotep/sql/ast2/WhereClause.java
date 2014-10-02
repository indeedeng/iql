package com.indeed.imhotep.sql.ast2;

import com.indeed.imhotep.sql.ast.Expression;
import com.indeed.imhotep.sql.ast.ValueObject;

import java.io.Serializable;

/**
 * @author vladimir
 */

public class WhereClause extends ValueObject implements Serializable {
    private final Expression expression;

    public WhereClause(Expression expression) {
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }
}
