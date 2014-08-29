package com.indeed.imhotep.sql.ast2;

import com.indeed.imhotep.sql.ast.Expression;
import com.indeed.imhotep.sql.ast.ValueObject;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * @author vladimir
 */

public class GroupByClause extends ValueObject implements Serializable {
    public final List<Expression> groupings;

    public GroupByClause(List<Expression> groupings) {
        this.groupings = groupings;
    }
}
