package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetGroupStats implements Command, JsonSerializable {
    public final List<AggregateMetric> metrics;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.returnGroupKeys = returnGroupKeys;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "getGroupStats");
        gen.writeObjectField("metrics", metrics);
        gen.writeArrayFieldStart("opts");
        if (returnGroupKeys) {
            gen.writeObject(ImmutableMap.of("type", "returnGroupKeys"));
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        for (final AggregateMetric metric : metrics) {
            metric.validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetGroupStats that = (GetGroupStats) o;
        return Objects.equals(returnGroupKeys, that.returnGroupKeys) &&
                Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, returnGroupKeys);
    }

    @Override
    public String toString() {
        return "GetGroupStats{" +
                "metrics=" + metrics +
                ", returnGroupKeys=" + returnGroupKeys +
                '}';
    }
}
