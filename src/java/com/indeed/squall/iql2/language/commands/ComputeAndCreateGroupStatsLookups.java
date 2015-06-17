package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.Lists;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ComputeAndCreateGroupStatsLookups implements Command, JsonSerializable {
    private final List<Pair<Command, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Command, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "computeAndCreateGroupStatsLookups");
        final List<List<Object>> computations = new ArrayList<>();
        for (final Pair<Command, String> pair : namedComputations) {
            computations.add(Lists.newArrayList(pair.getFirst(), pair.getSecond()));
        }
        gen.writeObjectField("computations", computations);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookups{" +
                "namedComputations=" + namedComputations +
                '}';
    }
}
