package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class FieldIterateOpts implements JsonSerializable {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();
    public Optional<Set<Long>> intTermSubset = Optional.absent();
    public Optional<Set<String>> stringTermSubset = Optional.absent();

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
            final Map<String, Object> topKMap = new HashMap<>();
            topKMap.put("type", "top");
            if (topK.limit.isPresent()) {
                topKMap.put("k", topK.limit.get());
            }
            if (topK.metric.isPresent()) {
                topKMap.put("metric", topK.metric.get());
            }
            gen.writeObject(topKMap);
        }
        if (intTermSubset.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "intTermSubset", "terms", intTermSubset.get()));
        }
        if (stringTermSubset.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "stringTermSubset", "terms", stringTermSubset.get()));
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
                Objects.equals(filter, that.filter) &&
                Objects.equals(intTermSubset, that.intTermSubset) &&
                Objects.equals(stringTermSubset, that.stringTermSubset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, topK, filter, intTermSubset, stringTermSubset);
    }

    @Override
    public String toString() {
        return "FieldIterateOpts{" +
                "limit=" + limit +
                ", topK=" + topK +
                ", filter=" + filter +
                ", intTermSubset=" + intTermSubset +
                ", stringTermSubset=" + stringTermSubset +
                '}';
    }
}
