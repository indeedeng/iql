package com.indeed.imhotep.sql.ast2;

import java.io.Serializable;

/**
 * @author vladimir
 */

public class DescribeStatement extends IQLStatement implements Serializable {
    public final String dataset;
    public final String field;

    public DescribeStatement(String dataset) {
        this.dataset = dataset;
        this.field = null;
    }

    public DescribeStatement(String dataset, String field) {
        this.dataset = dataset;
        this.field = field;
    }
}
