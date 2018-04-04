package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class ExtractQualifieds {
    public static Set<String> extractDocMetricDatasets(DocMetric docMetric) {
        final Set<String> aggregator = new HashSet<>();
        docMetric.transform(handleDocMetric(aggregator), handledocFilter(aggregator));
        return aggregator;
    }

    private static Function<DocFilter, DocFilter> handledocFilter(final Set<String> aggregator) {
        return new Function<DocFilter, DocFilter>() {
            @Nullable
            @Override
            public DocFilter apply(@Nullable DocFilter input) {
                if (input instanceof DocFilter.Qualified) {
                    aggregator.addAll(((DocFilter.Qualified) input).scope);
                }
                return null;
            }
        };
    }

    private static Function<DocMetric, DocMetric> handleDocMetric(final Set<String> aggregator) {
        return new Function<DocMetric, DocMetric>() {
            @Nullable
            @Override
            public DocMetric apply(@Nullable DocMetric input) {
                if (input instanceof DocMetric.Qualified) {
                    aggregator.add(((DocMetric.Qualified) input).dataset);
                }
                return input;
            }
        };
    }
}
