package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class ComputeFieldMax implements Command, JsonSerializable {
    private final Set<String> scope;
    private final String field;

    public ComputeFieldMax(Set<String> scope, String field) {
        this.scope = scope;
        this.field = field;
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        for (final String dataset : datasetsFields.datasets()) {
            if (!datasetsFields.getIntFields(dataset).contains(field)) {
                errorConsumer.accept(ErrorMessages.missingIntField(dataset, field, this));
            }
        }
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(ImmutableMap.of("command", "computeFieldMax", "scope", scope, "field", field));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeFieldMax that = (ComputeFieldMax) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, field);
    }

    @Override
    public String toString() {
        return "ComputeFieldMax{" +
                "field='" + field + '\'' +
                '}';
    }
}
