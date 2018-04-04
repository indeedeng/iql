package com.indeed.squall.iql2.language.query;

import com.google.common.base.Objects;

import java.util.List;

public class SplitQuery {
    public final String from;
    public final String where;
    public final String groupBy;
    public final String select;
    public final String limit;

    public final List<String> headers;
    public final List<String> groupBys;
    public final List<String> selects;

    public final String dataset;
    public final String start;
    public final String startRawString;
    public final String end;
    public final String endRawString;

    public final List<Dataset> datasets;

    public SplitQuery(String from, String where, String groupBy, String select, String limit, final List<String> headers, List<String> groupBys, List<String> selects,
                      String dataset, String start, String startRawString, String end, String endRawString, List<Dataset> datasets) {
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.select = select;
        this.limit = limit;

        this.headers = headers;
        this.groupBys = groupBys;
        this.selects = selects;

        this.dataset = dataset;
        this.start = start;
        this.startRawString = startRawString;
        this.end = end;
        this.endRawString = endRawString;
        this.datasets = datasets;
    }

    @Override
    public String toString() {
        return "SplitQuery{" +
                "from='" + from + '\'' +
                ", where='" + where + '\'' +
                ", groupBy='" + groupBy + '\'' +
                ", select='" + select + '\'' +
                ", limit='" + limit + '\'' +
                ", headers=" + headers +
                ", groupBys=" + groupBys +
                ", selects=" + selects +
                ", dataset='" + dataset + '\'' +
                ", start='" + start + '\'' +
                ", startRawString='" + startRawString + '\'' +
                ", end='" + end + '\'' +
                ", endRawString='" + endRawString + '\'' +
                ", datasets=" + datasets +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SplitQuery that = (SplitQuery) o;
        return Objects.equal(from, that.from) &&
                Objects.equal(where, that.where) &&
                Objects.equal(groupBy, that.groupBy) &&
                Objects.equal(select, that.select) &&
                Objects.equal(limit, that.limit) &&
                Objects.equal(headers, that.headers) &&
                Objects.equal(groupBys, that.groupBys) &&
                Objects.equal(selects, that.selects) &&
                Objects.equal(dataset, that.dataset) &&
                Objects.equal(start, that.start) &&
                Objects.equal(startRawString, that.startRawString) &&
                Objects.equal(end, that.end) &&
                Objects.equal(endRawString, that.endRawString) &&
                Objects.equal(datasets, that.datasets);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(from, where, groupBy, select, limit, headers, groupBys, selects, dataset, start, startRawString, end, endRawString, datasets);
    }

    static class Dataset {
        public final String name;
        public final String where;
        public final String start;
        public final String end;
        public final String alias;
        public final String fieldAlias;

        public Dataset(final String name, final String where, final String start, final String end, final String alias, final String fieldAlias) {
            this.name = name;
            this.where = where;
            this.start = start;
            this.end = end;
            this.alias = alias;
            this.fieldAlias = fieldAlias;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Dataset dataset = (Dataset) o;
            return Objects.equal(name, dataset.name) &&
                    Objects.equal(where, dataset.where) &&
                    Objects.equal(start, dataset.start) &&
                    Objects.equal(end, dataset.end) &&
                    Objects.equal(alias, dataset.alias) &&
                    Objects.equal(fieldAlias, dataset.fieldAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name, where, start, end, alias, fieldAlias);
        }

        @Override
        public String toString() {
            return "Dataset{" +
                    "dataset='" + name + '\'' +
                    ", where='" + where + '\'' +
                    ", start='" + start + '\'' +
                    ", end='" + end + '\'' +
                    ", alias='" + alias + '\'' +
                    ", fieldAlias='" + fieldAlias + '\'' +
                    '}';
        }
    }
}
