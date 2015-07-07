package com.indeed.squall.iql2.language.query;

public class SplitQuery {
    public final String from;
    public final String where;
    public final String groupBy;
    public final String select;
    public final String limit;

    public final String dataset;
    public final String start;
    public final String startRawString;
    public final String end;
    public final String endRawString;

    public SplitQuery(String from, String where, String groupBy, String select, String limit) {
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.select = select;
        this.limit = limit;

        this.dataset = "";
        this.start = "";
        this.startRawString = "";
        this.end = "";
        this.endRawString = "";
    }

    public SplitQuery(String from, String where, String groupBy, String select, String limit, String dataset, String start, String startRawString, String end, String endRawString) {
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.select = select;
        this.limit = limit;
        this.dataset = dataset;
        this.start = start;
        this.startRawString = startRawString;
        this.end = end;
        this.endRawString = endRawString;
    }
}
