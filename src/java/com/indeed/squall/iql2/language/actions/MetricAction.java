package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricAction implements Action, JsonSerializable {
    public final ImmutableSet<String> scope;
    public final DocFilter filter;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public MetricAction(Set<String> scope, DocFilter filter, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.filter = filter;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "metricAction");
        m.put("scope", scope);
        final Map<String, List<String>> perDatasetFilter = new HashMap<>();
        for (final String dataset : scope) {
            perDatasetFilter.put(dataset, new DocMetric.PushableDocMetric(filter.asZeroOneMetric(dataset)).getPushes(dataset));
        }
        m.put("perDatasetFilter", perDatasetFilter);
        m.put("target", targetGroup);
        m.put("positive", positiveGroup);
        m.put("negative", negativeGroup);
        gen.writeObject(m);
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        for (final String dataset : scope) {
            filter.validate(dataset, datasetsFields, validator);
        }
    }

    @Override
    public String toString() {
        return "MetricAction{" +
                "scope=" + scope +
                ", filter=" + filter +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
