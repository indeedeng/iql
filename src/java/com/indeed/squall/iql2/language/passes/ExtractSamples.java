package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExtractSamples {
    private static class FindSample implements Function<DocFilter, DocFilter> {
        boolean sampleFound = false;

        @Override
        public DocFilter apply(DocFilter input) {
            if (input instanceof DocFilter.Sample) {
                sampleFound = true;
            }
            return input;
        }
    }

    public static QueryResult extractSamples(Query query) {
        if (query.filter.isPresent()) {
            final Set<String> globalScope = query.extractDatasetNames();
            final Result result = extractSamples(query.filter.get(), globalScope);
            final Query newQuery = new Query(query.datasets, Optional.of(result.sampleFree), query.groupBys, query.selects, query.rowLimit);
            if (containsSamples(newQuery)) {
                throw new IllegalArgumentException("Query contains SAMPLE() filter outside of top spine of WHERE filter! [" + query + "]");
            }
            final List<ExecutionStep> executionSteps;
            if (result.perDatasetSamples.isEmpty()) {
                executionSteps = Collections.emptyList();
            } else {
                executionSteps = Collections.<ExecutionStep>singletonList(new ExecutionStep.SampleFields(result.perDatasetSamples));
            }
            return new QueryResult(newQuery, executionSteps);
        } else {
            return new QueryResult(query, Collections.<ExecutionStep>emptyList());
        }
    }

    public static Result extractSamples(DocFilter docFilter, final Set<String> globalScope) {
        final Map<String, List<DocFilter.Sample>> datasetSamples = new HashMap<>();
        final Function<DocFilter, DocFilter> processLevel = new Function<DocFilter, DocFilter>() {
            Set<String> scope = new HashSet<>(globalScope);
            @Override
            public DocFilter apply(DocFilter input) {
                if (input instanceof DocFilter.And) {
                    final DocFilter.And and = (DocFilter.And) input;
                    final DocFilter l = apply(and.f1);
                    final DocFilter r = apply(and.f2);
                    return new DocFilter.And(l, r);
                } else if (input instanceof DocFilter.Qualified) {
                    final DocFilter.Qualified qualified = (DocFilter.Qualified) input;
                    final Set<String> oldScope = this.scope;
                    this.scope = new HashSet<>(qualified.scope);
                    apply(qualified.filter);
                    this.scope = oldScope;
                } else if (input instanceof DocFilter.Sample) {
                    for (final String dataset : scope) {
                        List<DocFilter.Sample> samples = datasetSamples.get(dataset);
                        if (samples == null) {
                            samples = new ArrayList<>();
                            datasetSamples.put(dataset, samples);
                        }
                        samples.add((DocFilter.Sample) input);
                    }
                    return new DocFilter.Always();
                }
                return input;
            }
        };
        final DocFilter newFilter = processLevel.apply(docFilter);
        if (containsSamples(newFilter)) {
            throw new IllegalArgumentException("Sample() metrics found outside of AND-QUALIFIED spine of filter: " + docFilter);
        }
        return new Result(newFilter, datasetSamples);
    }

    public static boolean containsSamples(DocFilter filter) {
        final FindSample findSample = new FindSample();
        filter.transform(Functions.<DocMetric>identity(), findSample);
        return findSample.sampleFound;
    }

    public static boolean containsSamples(Query query) {
        final FindSample findSample = new FindSample();
        query.transform(
                Functions.<GroupBy>identity(),
                Functions.<AggregateMetric>identity(),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                findSample
        );
        return findSample.sampleFound;
    }

    public static class Result {
        public final DocFilter sampleFree; // Not the same as freeSamples
        public final Map<String, List<DocFilter.Sample>> perDatasetSamples;

        private Result(DocFilter sampleFree, Map<String, List<DocFilter.Sample>> perDatasetSamples) {
            this.sampleFree = sampleFree;
            this.perDatasetSamples = perDatasetSamples;
        }
    }

    public static class QueryResult {
        public final Query query;
        public final List<ExecutionStep> executionSteps;

        public QueryResult(Query query, List<ExecutionStep> executionSteps) {
            this.query = query;
            this.executionSteps = executionSteps;
        }

        @Override
        public String toString() {
            return "QueryResult{" +
                    "query=" + query +
                    ", executionSteps=" + executionSteps +
                    '}';
        }
    }
}
