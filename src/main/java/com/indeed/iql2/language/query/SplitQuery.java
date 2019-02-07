/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.language.query;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
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

    public SplitQuery(final String from, final String where, final String groupBy, final String select, final String limit, final List<String> headers, final List<String> groupBys, final List<String> selects, final String dataset, final String start, final String startRawString, final String end, final String endRawString, final List<Dataset> datasets) {
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

    @EqualsAndHashCode
    @ToString
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
    }
}
