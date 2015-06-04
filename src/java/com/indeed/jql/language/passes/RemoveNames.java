package com.indeed.jql.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.query.GroupBy;
import com.indeed.jql.language.query.Query;

import javax.annotation.Nullable;
import java.util.Map;

public class RemoveNames {
    public static Query removeNames(Query query) {
        return query.traverse(
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
