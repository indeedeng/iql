package com.indeed.imhotep.sql.ast2;

import com.indeed.imhotep.sql.ast.ValueObject;
import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * @author vladimir
 */

public class FromClause extends ValueObject implements Serializable {
    private final String dataset;
    private final DateTime start;
    private final DateTime end;
    private final String startRawString;
    private final String endRawString;

    public FromClause(String dataset, DateTime start, DateTime end) {
        this(dataset, start, end, start.toString(), end.toString());
    }

    public FromClause(String dataset, DateTime start, DateTime end, String startRawString, String endRawString) {
        this.dataset = dataset;
        this.start = start;
        this.end = end;
        this.startRawString = startRawString;
        this.endRawString = endRawString;
    }

    public String getDataset() {
        return dataset;
    }

    public DateTime getStart() {
        return start;
    }

    public DateTime getEnd() {
        return end;
    }

    public String getStartRawString() {
        return startRawString;
    }

    public String getEndRawString() {
        return endRawString;
    }

    /**
     * start/end raw strings are ignored in comparisons and toString.
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FromClause)) return false;

        FromClause that = (FromClause) o;

        if (dataset != null ? !dataset.equals(that.dataset) : that.dataset != null) return false;
        if (end != null ? !end.isEqual(that.end) : that.end != null) return false;
        if (start != null ? !start.isEqual(that.start) : that.start != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (dataset != null ? dataset.hashCode() : 0);
        result = 31 * result + (start != null ? start.hashCode() : 0);
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FromClause {dataset=" + dataset + ", end=" + end.toString() + ", start=" + start.toString() + "}";
    }
}
