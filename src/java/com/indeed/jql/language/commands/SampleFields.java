package com.indeed.jql.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.jql.language.DocFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleFields implements Command, JsonSerializable {
    private final Map<String, List<DocFilter.Sample>> perDatasetSamples;

    public SampleFields(Map<String, List<DocFilter.Sample>> perDatasetSamples) {
        this.perDatasetSamples = perDatasetSamples;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, List<Map<String, Object>>> perDatasetSamples = new HashMap<>();
        for (final Map.Entry<String, List<DocFilter.Sample>> entry : this.perDatasetSamples.entrySet()) {
            final List<Map<String, Object>> l = new ArrayList<>();
            for (final DocFilter.Sample sample : entry.getValue()) {
                l.add(ImmutableMap.<String, Object>of("field", sample.field, "fraction", ((double) sample.numerator) / sample.denominator, "seed", sample.seed));
            }
            perDatasetSamples.put(entry.getKey(), l);
        }
        gen.writeObject(ImmutableMap.of("command", "sampleFields", "perDatasetSamples", perDatasetSamples));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "SampleFields{" +
                "perDatasetSamples=" + perDatasetSamples +
                '}';
    }
}
