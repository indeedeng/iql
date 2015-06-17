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
import java.util.Map;

public class SubstituteNamed {
    public static Query substituteNamedMetrics(Query query, Map<String, AggregateMetric> namedMetrics) {
        return query.transform(
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
                    if (implicitDocStats.docMetric instanceof DocMetric.Field) {
                        final DocMetric.Field docMetric = (DocMetric.Field) implicitDocStats.docMetric;
                        if (namedMetrics.containsKey(docMetric.field)) {
                            return namedMetrics.get(docMetric.field);
                        }
                    }
                }
                return input;
            }
        };
    }
}
