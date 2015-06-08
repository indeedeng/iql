package com.indeed.jql.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;

import java.io.IOException;

public class ComputeAndCreateGroupStatsLookup implements Command, JsonSerializable {
    public final Command computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Command computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "computeAndCreateGroupStatsLookup");
        gen.writeObjectField("computation", computation);
        gen.writeStringField("name", name.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookup{" +
                "computation=" + computation +
                ", name=" + name +
                '}';
    }
}
