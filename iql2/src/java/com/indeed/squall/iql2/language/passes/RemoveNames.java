package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import javax.annotation.Nullable;

public class RemoveNames {
    public static Query removeNames(Query query) {
        return query.transform(
                Functions.<GroupBy>identity(),
                removeNames(),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity()
        );
    }

    private static Function<AggregateMetric, AggregateMetric> removeNames() {
        return new Function<AggregateMetric, AggregateMetric>() {
            @Nullable @Override
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.Named) {
                    return ((AggregateMetric.Named) input).metric;
                } else {
                    return input;
                }
            }
        };
    }
}
