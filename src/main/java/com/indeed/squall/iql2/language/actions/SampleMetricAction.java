package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleMetricAction implements Action, JsonSerializable {
    final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final Map<String, List<String>> perDatasetMetrics,
                              final double probability,
                              final String seed,
                              final int targetGroup,
                              final int positiveGroup,
                              final int negativeGroup) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetrics.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void serialize(final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "sampleMetricAction");
        m.put("perDatasetMetric", perDatasetMetric);
        m.put("probability", probability);
        m.put("seed", seed);
        m.put("target", targetGroup);
        m.put("positive", positiveGroup);
        m.put("negative", negativeGroup);
        gen.writeObject(m);
    }

    @Override
    public void serializeWithType(final JsonGenerator gen, final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
    }

    @Override
    public com.indeed.squall.iql2.execution.actions.Action toExecutionAction(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.squall.iql2.execution.actions.SampleMetricAction(
                perDatasetMetric,
                probability,
                seed,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }

    @Override
    public String toString() {
        return "SampleMetricAction{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
