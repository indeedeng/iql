package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimpleIterate extends RequiresFTGSCommand {
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    private final List<Optional<String>> formatStrings;
    public final boolean streamResult;

    public SimpleIterate(String field, FieldIterateOpts opts, List<AggregateMetric> selecting, List<Optional<String>> formatStrings, boolean streamResult) {
        super(field);
        this.opts = opts;
        this.selecting = selecting;
        this.formatStrings = formatStrings;
        this.streamResult = streamResult;
        if (this.streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Can't stream results while doing top-k!");
        }
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> obj = new HashMap<>();
        obj.put("command", "simpleIterate");
        obj.put("field", field);
        obj.put("opts", opts);
        obj.put("selects", selecting);
        obj.put("streamResult", streamResult);
        final List<String> serializableFormatStrings = new ArrayList<>(formatStrings.size());
        for (final Optional<String> opt : formatStrings) {
            serializableFormatStrings.add(opt.orNull());
        }
        obj.put("formatStrings", serializableFormatStrings);
        gen.writeObject(obj);
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        super.validate(datasetsFields, validator);
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                topK.metric.get().validate(datasetsFields.datasets(), datasetsFields, validator);
            }
        }

        if (opts.filter.isPresent()) {
            opts.filter.get().validate(datasetsFields.datasets(), datasetsFields, validator);
        }

        for (final AggregateMetric metric : selecting) {
            metric.validate(datasetsFields.datasets(), datasetsFields, validator);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleIterate that = (SimpleIterate) o;
        return streamResult == that.streamResult &&
                Objects.equals(field, that.field) &&
                Objects.equals(opts, that.opts) &&
                Objects.equals(selecting, that.selecting) &&
                Objects.equals(formatStrings, that.formatStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, opts, selecting, formatStrings, streamResult);
    }

    @Override
    public String toString() {
        return "SimpleIterate{" +
                "field='" + field + '\'' +
                ", opts=" + opts +
                ", selecting=" + selecting +
                ", formatStrings=" + formatStrings +
                ", streamResult=" + streamResult +
                '}';
    }
}
