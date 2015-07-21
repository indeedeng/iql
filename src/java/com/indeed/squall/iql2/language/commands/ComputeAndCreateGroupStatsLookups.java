package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeAndCreateGroupStatsLookups that = (ComputeAndCreateGroupStatsLookups) o;
        return Objects.equals(namedComputations, that.namedComputations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namedComputations);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookups{" +
                "namedComputations=" + namedComputations +
                '}';
    }
}
