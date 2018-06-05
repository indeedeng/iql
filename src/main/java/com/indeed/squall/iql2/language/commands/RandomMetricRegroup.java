package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RandomMetricRegroup implements Command, JsonSerializable {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    private final int k;
    private final String salt;

    public RandomMetricRegroup(final Map<String, List<String>> perDatasetMetric,
                               final int k,
                               final String salt) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.RandomMetricRegroup(
                perDatasetMetric,
                k,
                salt
        );
    }

    @Override
    public void serialize(final JsonGenerator gen, final SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "randomMetricRegroup");
        gen.writeObjectField("perDatasetMetric", perDatasetMetric);
        gen.writeNumberField("k", k);
        gen.writeStringField("salt", salt);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(final JsonGenerator jsonGenerator,
                                  final SerializerProvider serializerProvider,
                                  final TypeSerializer typeSerializer) throws IOException {
        this.serialize(jsonGenerator, serializerProvider);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final RandomMetricRegroup that = (RandomMetricRegroup) o;
        return Objects.equal(perDatasetMetric, that.perDatasetMetric) && (k == that.k) && Objects.equal(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(perDatasetMetric, k, salt);
    }

    @Override
    public String toString() {
        return "RandomMetricRegroup{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", k=" + k +
                ", salt='" + salt + '\'' +
                '}';
    }
}
