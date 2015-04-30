package com.indeed.squall.jql;

import java.util.Arrays;

/**
* @author jwolfe
*/
public class TermSelects {
    public final String field;

    public boolean isIntTerm;
    public String stringTerm;
    public long intTerm;

    public final double[] selects;
    public double topMetric;
    public final Session.GroupKey groupKey;

    TermSelects(String field, boolean isIntTerm, String stringTerm, long intTerm, double[] selects, double topMetric, Session.GroupKey groupKey) {
        this.field = field;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
        this.selects = selects;
        this.topMetric = topMetric;
        this.groupKey = groupKey;
    }

    @Override
    public String toString() {
        return "com.indeed.squall.jql.TermSelects{" +
                "isIntTerm=" + isIntTerm +
                ", stringTerm='" + stringTerm + '\'' +
                ", intTerm=" + intTerm +
                ", selects=" + Arrays.toString(selects) +
                ", topMetric=" + topMetric +
                ", groupKey=" + groupKey +
                '}';
    }
}
