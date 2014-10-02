package com.indeed.imhotep.sql.ast2;

import com.indeed.imhotep.sql.ast.Expression;
import com.indeed.imhotep.sql.ast.ValueObject;

import java.io.Serializable;
import java.util.List;

/**
 * @author vladimir
 */

public class SelectClause extends ValueObject implements Serializable {
    private final List<Expression> projections;

    public SelectClause(List<Expression> projections) {
        this.projections = projections;
    }

    public List<Expression> getProjections() {
        return projections;
    }
}
