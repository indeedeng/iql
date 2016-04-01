package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.Objects;

public class ApplyGroupFilter implements Command, JsonSerializable {
    private final AggregateFilter filter;

    public ApplyGroupFilter(AggregateFilter filter) {
        this.filter = filter;
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        // TODO: Validate.
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "applyGroupFilter");
        gen.writeObjectField("filter", filter);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplyGroupFilter that = (ApplyGroupFilter) o;
        return Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter);
    }

    @Override
    public String toString() {
        return "ApplyGroupFilter{" +
                "filter=" + filter +
                '}';
    }
}
