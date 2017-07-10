package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FilterDocs implements Command, JsonSerializable {
    public final ImmutableMap<String, DocMetric.PushableDocMetric> perDatasetFilterMetric;

    public FilterDocs(Map<String, DocMetric.PushableDocMetric> perDatasetFilterMetric) {
        this.perDatasetFilterMetric = ImmutableMap.copyOf(perDatasetFilterMetric);
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "filterDocs");
        gen.writeObjectField("perDatasetFilter", getPushesMap());
        gen.writeEndObject();
    }

    private Map<String, List<String>> getPushesMap() {
        final Map<String, List<String>> result = new HashMap<>();
        for (final Map.Entry<String, DocMetric.PushableDocMetric> entry : perDatasetFilterMetric.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getPushes(entry.getKey()));
        }
        return result;
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final Map.Entry<String, DocMetric.PushableDocMetric> entry : perDatasetFilterMetric.entrySet()) {
            entry.getValue().validate(entry.getKey(), validationHelper, validator);
        }
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
