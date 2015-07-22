package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;

public class IterateAndExplode implements Command, JsonSerializable {
    public final String field;
    public final FieldIterateOpts fieldOpts;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(String field, FieldIterateOpts fieldOpts, Optional<String> explodeDefaultName) {
        this.field = field;
        this.fieldOpts = fieldOpts;
        this.explodeDefaultName = explodeDefaultName;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "iterateAndExplode");
        gen.writeStringField("field", field);
        gen.writeArrayFieldStart("iterOpts");
        gen.writeObject(ImmutableMap.of("type", "defaultedFieldOpts", "opts", fieldOpts));
        gen.writeEndArray();
        gen.writeArrayFieldStart("explodeOpts");
        if (explodeDefaultName.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "addDefault", "name", explodeDefaultName.get()));
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
        for (final String dataset : datasetsFields.datasets()) {
            if (!datasetsFields.getAllFields(dataset).contains(field)) {
                errorConsumer.accept(ErrorMessages.missingField(dataset, field, this));
            }
        }

        if (fieldOpts.topK.isPresent()) {
            fieldOpts.topK.get().metric.validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }

        if (fieldOpts.filter.isPresent()) {
            fieldOpts.filter.get().validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterateAndExplode that = (IterateAndExplode) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(fieldOpts, that.fieldOpts) &&
                Objects.equals(explodeDefaultName, that.explodeDefaultName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, fieldOpts, explodeDefaultName);
    }

    @Override
    public String toString() {
        return "IterateAndExplode{" +
                "field='" + field + '\'' +
                ", fieldOpts=" + fieldOpts +
                ", explodeDefaultName=" + explodeDefaultName +
                '}';
    }
}
