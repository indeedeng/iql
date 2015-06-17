package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateMetric;

import java.io.IOException;
import java.util.List;

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
    public String toString() {
        return "GetGroupStats{" +
                "metrics=" + metrics +
                ", returnGroupKeys=" + returnGroupKeys +
                '}';
    }
}
