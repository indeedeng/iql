package com.indeed.jql.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;

import java.util.ArrayList;
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

    public Result apply(DocFilter docFilter, final Set<String> globalScope) {
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

    public static class Result {
        public final DocFilter sampleFree; // Not the same as freeSamples
        public final Map<String, List<DocFilter.Sample>> perDatasetSamples;

        private Result(DocFilter sampleFree, Map<String, List<DocFilter.Sample>> perDatasetSamples) {
            this.sampleFree = sampleFree;
            this.perDatasetSamples = perDatasetSamples;
        }
    }
}
