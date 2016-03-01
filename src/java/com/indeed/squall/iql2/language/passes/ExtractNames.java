package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import java.util.HashMap;
import java.util.Map;

public class ExtractNames {
    public static Map<String, AggregateMetric> extractNames(Query query) {
        final Map<String, AggregateMetric> result = new HashMap<>();
        query.transform(
                Functions.<GroupBy>identity(),
                handleAggregateMetric(result),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity()
        );
        return result;
    }

    public static Function<AggregateMetric, AggregateMetric> handleAggregateMetric(final Map<String, AggregateMetric> resultAggregator) {
        return new Function<AggregateMetric, AggregateMetric>() {
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.Named) {
                    final AggregateMetric.Named named = (AggregateMetric.Named) input;
                    if (resultAggregator.containsKey(named.name)) {
                        throw new IllegalArgumentException("Trying to name multiple metrics the same name: [" + named.name + "]!");
                    }
                    resultAggregator.put(named.name, named.metric);
                }
                return input;
            }
        };
    }
}
