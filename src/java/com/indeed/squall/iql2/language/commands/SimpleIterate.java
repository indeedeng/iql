package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SimpleIterate implements Command, JsonSerializable {
    public final String field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final boolean streamResult;

    public SimpleIterate(String field, FieldIterateOpts opts, List<AggregateMetric> selecting, boolean streamResult) {
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

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        for (final String dataset : datasetsFields.datasets()) {
            if (!datasetsFields.getAllFields(dataset).contains(field)) {
                errorConsumer.accept(ErrorMessages.missingField(dataset, field, this));
            }
        }

        if (opts.topK.isPresent()) {
            opts.topK.get().metric.validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }

        if (opts.filter.isPresent()) {
            opts.filter.get().validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }

        for (final AggregateMetric metric : selecting) {
            metric.validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleIterate that = (SimpleIterate) o;
        return Objects.equals(streamResult, that.streamResult) &&
                Objects.equals(field, that.field) &&
                Objects.equals(opts, that.opts) &&
                Objects.equals(selecting, that.selecting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, opts, selecting, streamResult);
    }

    @Override
    public String toString() {
        return "SimpleIterate{" +
                "field='" + field + '\'' +
                ", opts=" + opts +
                ", selecting=" + selecting +
                ", streamResult=" + streamResult +
                '}';
    }
}
