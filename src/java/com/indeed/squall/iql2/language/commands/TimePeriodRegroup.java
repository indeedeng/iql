package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;

public class TimePeriodRegroup implements Command, JsonSerializable {
    private final long periodMillis;
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;
    private final boolean isRelative;

    public TimePeriodRegroup(long periodMillis, Optional<String> timeField, Optional<String> timeFormat, boolean isRelative) {
        this.periodMillis = periodMillis;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.isRelative = isRelative;
    }


    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "timePeriodRegroup");
        gen.writeBooleanField("isRelative", isRelative);
        gen.writeNumberField("periodMillis", periodMillis);
        gen.writeStringField("timeField", timeField.orNull());
        gen.writeStringField("timeFormat", timeFormat.orNull());

        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        if (timeField.isPresent()) {
            for (final String dataset : datasetsFields.datasets()) {
                if (!datasetsFields.getAllFields(dataset).contains(timeField.get())) {
                    validator.error(ErrorMessages.missingField(dataset, timeField.get(), this));
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePeriodRegroup that = (TimePeriodRegroup) o;
        return Objects.equals(periodMillis, that.periodMillis) &&
                Objects.equals(timeField, that.timeField) &&
                Objects.equals(timeFormat, that.timeFormat) &&
                Objects.equals(isRelative, that.isRelative);
    }

    @Override
    public int hashCode() {
        return Objects.hash(periodMillis, timeField, timeFormat, isRelative);
    }

    @Override
    public String toString() {
        return "TimePeriodRegroup{" +
                "periodMillis=" + periodMillis +
                ", timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                ", isRelative=" + isRelative +
                '}';
    }

}
