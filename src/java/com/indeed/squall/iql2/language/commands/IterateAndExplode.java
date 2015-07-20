package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IterateAndExplode implements Command, JsonSerializable {
    public final String field;
    public final List<AggregateMetric> selecting;
    public final FieldIterateOpts fieldOpts;
    public final Optional<Pair<Integer, FieldLimitingMechanism>> fieldLimits;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(String field, List<AggregateMetric> selecting, FieldIterateOpts fieldOpts, Optional<Pair<Integer, FieldLimitingMechanism>> fieldLimits, Optional<String> explodeDefaultName) {
        this.field = field;
        this.selecting = selecting;
        this.fieldOpts = fieldOpts;
        this.fieldLimits = fieldLimits;
        this.explodeDefaultName = explodeDefaultName;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "iterateAndExplode");
        gen.writeStringField("field", field);
        gen.writeArrayFieldStart("iterOpts");
        gen.writeObject(ImmutableMap.of("type", "defaultedFieldOpts", "opts", fieldOpts));
        if (fieldLimits.isPresent()) {
            final Pair<Integer, FieldLimitingMechanism> p = fieldLimits.get();
            gen.writeObject(ImmutableMap.of("type", "limitingFields", "numFields", p.getFirst(), "by", p.getSecond()));
        }
        if (!selecting.isEmpty()) {
            gen.writeObject(ImmutableMap.of("type", "selecting", "metrics", selecting));
        }
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
    public void validate(Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields, Consumer<String> errorConsumer) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterateAndExplode that = (IterateAndExplode) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(selecting, that.selecting) &&
                Objects.equals(fieldOpts, that.fieldOpts) &&
                Objects.equals(fieldLimits, that.fieldLimits) &&
                Objects.equals(explodeDefaultName, that.explodeDefaultName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, selecting, fieldOpts, fieldLimits, explodeDefaultName);
    }

    @Override
    public String toString() {
        return "IterateAndExplode{" +
                "field='" + field + '\'' +
                ", selecting=" + selecting +
                ", fieldOpts=" + fieldOpts +
                ", fieldLimits=" + fieldLimits +
                ", explodeDefaultName=" + explodeDefaultName +
                '}';
    }
}
