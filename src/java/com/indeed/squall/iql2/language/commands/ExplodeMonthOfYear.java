package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.language.compat.Consumer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ExplodeMonthOfYear implements Command, JsonSerializable {
    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeMonthOfYear");
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields, Consumer<String> errorConsumer) {

    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ExplodeMonthOfYear;
    }

    @Override
    public int hashCode() {
        // TODO: ???
        return 83;
    }

    @Override
    public String toString() {
        return "ExplodeMonthOfYear{}";
    }
}
