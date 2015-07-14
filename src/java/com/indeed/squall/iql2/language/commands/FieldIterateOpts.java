package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateFilter;

import java.io.IOException;
import java.util.Objects;

public class FieldIterateOpts implements JsonSerializable {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();

    public FieldIterateOpts copy() {
        final FieldIterateOpts result = new FieldIterateOpts();
        result.limit = this.limit;
        result.topK = this.topK;
        result.filter = this.filter;
        return result;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        if (filter.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "filter", "filter", filter.get()));
        }
        if (limit.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "limit", "k", limit.get()));
        }
        if (topK.isPresent()) {
            final TopK topK = this.topK.get();
            gen.writeObject(ImmutableMap.of("type", "top", "k", topK.limit, "metric", topK.metric));
        }
        gen.writeEndArray();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldIterateOpts that = (FieldIterateOpts) o;
        return Objects.equals(limit, that.limit) &&
                Objects.equals(topK, that.topK) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, topK, filter);
    }

    @Override
    public String toString() {
        return "FieldIterateOpts{" +
                "limit=" + limit +
                ", topK=" + topK +
                ", filter=" + filter +
                '}';
    }
}
