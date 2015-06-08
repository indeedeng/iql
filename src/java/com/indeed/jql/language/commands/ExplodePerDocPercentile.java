package com.indeed.jql.language.commands;

public class ExplodePerDocPercentile implements Command {
    public final String field;
    public final int numBuckets;

    public ExplodePerDocPercentile(String field, int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    @Override
    public String toString() {
        return "ExplodePerDocPercentile{" +
                "field='" + field + '\'' +
                ", numBuckets=" + numBuckets +
                '}';
    }
}
