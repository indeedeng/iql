package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;

import java.io.IOException;
import java.util.Set;

public class GetGroupDistincts implements Command, JsonSerializable {
    public final Set<String> scope;
    public final String field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistincts(Set<String> scope, String field, Optional<AggregateFilter> filter, int windowSize) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "getGroupDistincts");
        gen.writeObjectField("scope", scope);
        gen.writeStringField("field", field);
        gen.writeObjectField("filter", filter.orNull());
        gen.writeNumberField("windowSize", windowSize);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "GetGroupDistincts{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", filter=" + filter +
                ", windowSize=" + windowSize +
                '}';
    }
}
