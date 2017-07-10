package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import java.util.ArrayList;
import java.util.List;

public class FixTopKHaving {
    public static Query apply(Query query) {
        final List<GroupByMaybeHaving> newGroupBys = new ArrayList<>();
        for (final GroupByMaybeHaving groupBy : query.groupBys) {
            if (groupBy.groupBy instanceof GroupBy.GroupByField && groupBy.filter.isPresent()) {
                final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy.groupBy;
                final AggregateFilter newFilter;
                if (groupByField.filter.isPresent()) {
                    newFilter = new AggregateFilter.And(groupByField.filter.get(), groupBy.filter.get());
                } else {
                    newFilter = groupBy.filter.get();
                }
                final GroupBy.GroupByField newGroupBy = new GroupBy.GroupByField(groupByField.field, Optional.of(newFilter), groupByField.limit, groupByField.metric, groupByField.withDefault, groupByField.forceNonStreaming);
                newGroupBys.add(new GroupByMaybeHaving(newGroupBy, Optional.<AggregateFilter>absent()));
            } else {
                newGroupBys.add(groupBy);
            }
        }
        return new Query(query.datasets, query.filter, newGroupBys, query.selects, query.formatStrings, query.rowLimit, query.useLegacy);
    }
}
