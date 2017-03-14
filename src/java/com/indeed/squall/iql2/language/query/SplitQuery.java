package com.indeed.squall.iql2.language.query;

import java.util.List;

public class SplitQuery {
    public final String from;
    public final String where;
    public final String groupBy;
    public final String select;
    public final String limit;

    public final List<String> groupBys;
    public final List<String> selects;

    public final String dataset;
    public final String start;
    public final String startRawString;
    public final String end;
    public final String endRawString;

    public SplitQuery(String from, String where, String groupBy, String select, String limit, List<String> groupBys, List<String> selects) {
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.select = select;
        this.limit = limit;

        this.groupBys = groupBys;
        this.selects = selects;

        this.dataset = "";
        this.start = "";
        this.startRawString = "";
        this.end = "";
        this.endRawString = "";
    }

    public SplitQuery(String from, String where, String groupBy, String select, String limit, List<String> groupBys, List<String> selects, String dataset, String start, String startRawString, String end, String endRawString) {
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.select = select;
        this.limit = limit;

        this.groupBys = groupBys;
        this.selects = selects;

        this.dataset = dataset;
        this.start = start;
        this.startRawString = startRawString;
        this.end = end;
        this.endRawString = endRawString;
    }
}
