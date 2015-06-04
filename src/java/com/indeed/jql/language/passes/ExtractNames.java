package com.indeed.jql.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Sets;
import com.google.gag.annotation.remark.ThisWouldBeOneLineIn;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.JQLLexer;
import com.indeed.jql.language.JQLParser;
import com.indeed.jql.language.query.GroupBy;
import com.indeed.jql.language.query.Query;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class ExtractNames {
    @ThisWouldBeOneLineIn (
        language = "haskell",
        toWit = "M.fromList [ (name, metric) | Aggregate.Named metric name <- universeBi q ]"
    )
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
                        throw new IllegalArgumentException("Trying to name multiple metrics [" + named.name + "]!");
                    }
                    resultAggregator.put(named.name, named.metric);
                }
                return input;
            }
        };
    }
}
