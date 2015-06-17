package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;

import java.io.IOException;
import java.util.Arrays;

public class CreateGroupStatsLookup implements Command, JsonSerializable {
    public final double[] stats;
    public final Optional<String> name;

    public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
        this.stats = stats;
        this.name = name;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "createGroupStatsLookup");
        gen.writeObjectField("values", stats);
        gen.writeStringField("name", name.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "CreateGroupStatsLookup{" +
                "stats=" + Arrays.toString(stats) +
                ", name=" + name +
                '}';
    }
}
