package com.indeed.jql.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.List;

public class IterateAndExplode implements Command, JsonSerializable {
    public final String field;
    public final List<AggregateMetric> selecting;
    public final Iterate.FieldIterateOpts fieldOpts;
    public final Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(String field, List<AggregateMetric> selecting, Iterate.FieldIterateOpts fieldOpts, Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits, Optional<String> explodeDefaultName) {
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
            final Pair<Integer, Iterate.FieldLimitingMechanism> p = fieldLimits.get();
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
