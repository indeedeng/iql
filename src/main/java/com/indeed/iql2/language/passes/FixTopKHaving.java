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

package com.indeed.iql2.language.passes;

import com.google.common.base.Optional;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;

import java.util.ArrayList;
import java.util.List;

public class FixTopKHaving {
    public static Query apply(Query query) {
        final List<GroupByEntry> newGroupBys = new ArrayList<>();
        for (final GroupByEntry groupBy : query.groupBys) {
            if (groupBy.groupBy instanceof GroupBy.GroupByField && groupBy.filter.isPresent()) {
                final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy.groupBy;
                final AggregateFilter newFilter;
                if (groupByField.filter.isPresent()) {
                    newFilter = new AggregateFilter.And(groupByField.filter.get(), groupBy.filter.get());
                } else {
                    newFilter = groupBy.filter.get();
                }
                final GroupBy.GroupByField newGroupBy = new GroupBy.GroupByField(groupByField.field, Optional.of(newFilter), groupByField.limit, groupByField.metric, groupByField.withDefault, groupByField.forceNonStreaming);
                newGroupBys.add(new GroupByEntry(newGroupBy, Optional.<AggregateFilter>absent(), Optional.<String>absent()));
            } else {
                newGroupBys.add(groupBy);
            }
        }
        return new Query(query.datasets, query.filter, newGroupBys, query.selects, query.formatStrings, query.options, query.rowLimit, query.useLegacy);
    }
}
