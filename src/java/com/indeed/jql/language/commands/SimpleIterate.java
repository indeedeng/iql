package com.indeed.jql.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.jql.language.AggregateMetric;

import java.io.IOException;
import java.util.List;

public class SimpleIterate implements Command, JsonSerializable {
    public final String field;
    public final Iterate.FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final boolean streamResult;

    public SimpleIterate(String field, Iterate.FieldIterateOpts opts, List<AggregateMetric> selecting, boolean streamResult) {
        this.field = field;
        this.opts = opts;
        this.selecting = selecting;
        this.streamResult = streamResult;
        if (this.streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Can't stream results while doing top-k!");
        }
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(ImmutableMap.of("command", "simpleIterate", "field", field, "opts", opts, "selects", selecting, "streamResult", streamResult));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }
}
