package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.indeed.squall.iql2.language.compat.Consumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class FilterDocs implements Command, JsonSerializable {
    public final Map<String, List<String>> perDatasetFilterMetric;

    public FilterDocs(Map<String, List<String>> perDatasetFilterMetric) {
        final Map<String, List<String>> copy = Maps.newHashMap();
        for (final Map.Entry<String, List<String>> entry : perDatasetFilterMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetFilterMetric = copy;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "filterDocs");
        gen.writeObjectField("perDatasetFilter", perDatasetFilterMetric);
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
        FilterDocs that = (FilterDocs) o;
        return Objects.equals(perDatasetFilterMetric, that.perDatasetFilterMetric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(perDatasetFilterMetric);
    }

    @Override
    public String toString() {
        return "FilterDocs{" +
                "perDatasetFilterMetric=" + perDatasetFilterMetric +
                '}';
    }
}
