package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;

public class ExplodeTimeBuckets implements Command, JsonSerializable {
    private final int numBuckets;
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;

    public ExplodeTimeBuckets(int numBuckets, Optional<String> timeField, Optional<String> timeFormat) {
        this.numBuckets = numBuckets;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeTimeBuckets");
        gen.writeNumberField("numBuckets", numBuckets);
        gen.writeStringField("timeField", timeField.orNull());
        gen.writeStringField("timeFormat", timeFormat.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        if (timeField.isPresent()) {
            for (final String dataset : datasetsFields.datasets()) {
                if (!datasetsFields.getAllFields(dataset).contains(timeField.get())) {
                    errorConsumer.accept(ErrorMessages.missingField(dataset, timeField.get(), this));
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodeTimeBuckets that = (ExplodeTimeBuckets) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(timeField, that.timeField) &&
                Objects.equals(timeFormat, that.timeFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numBuckets, timeField, timeFormat);
    }

    @Override
    public String toString() {
        return "ExplodeTimeBuckets{" +
                "numBuckets=" + numBuckets +
                ", timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
