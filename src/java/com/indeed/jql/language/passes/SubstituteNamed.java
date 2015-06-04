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

public class SubstituteNamed {
    public static Query substituteNamedMetrics(Query query, Map<String, AggregateMetric> namedMetrics) {
        return query.traverse(
                Functions.<GroupBy>identity(),
                replaceNamed(namedMetrics),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity()
        );
    }

    private static Function<AggregateMetric, AggregateMetric> replaceNamed(final Map<String, AggregateMetric> namedMetrics) {
        return new Function<AggregateMetric, AggregateMetric>() {
            @Nullable @Override
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.ImplicitDocStats) {
                    final AggregateMetric.ImplicitDocStats implicitDocStats = (AggregateMetric.ImplicitDocStats) input;
                    if (namedMetrics.containsKey(implicitDocStats.field)) {
                        return namedMetrics.get(implicitDocStats.field);
                    } else {
                        return input;
                    }
                } else {
                    return input;
                }
            }
        };
    }
}
