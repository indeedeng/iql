package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.Objects;

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
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        computation.validate(datasetsFields, errorConsumer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeAndCreateGroupStatsLookup that = (ComputeAndCreateGroupStatsLookup) o;
        return Objects.equals(computation, that.computation) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(computation, name);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookup{" +
                "computation=" + computation +
                ", name=" + name +
                '}';
    }
}
