package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetricRegroup implements Command, JsonSerializable {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;
    public final boolean withDefault;
    public final boolean fromPredict;

    public MetricRegroup(Map<String, List<String>> perDatasetMetric, long min, long max, long interval, boolean excludeGutters, boolean withDefault, boolean fromPredict) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.min = min;
        this.max = max;
        this.interval = interval;
        this.excludeGutters = excludeGutters;
        this.withDefault = withDefault;
        this.fromPredict = fromPredict;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "metricRegroup");
        gen.writeObjectField("perDatasetMetric", perDatasetMetric);
        gen.writeNumberField("min", min);
        gen.writeNumberField("max", max);
        gen.writeNumberField("interval", interval);
        if (excludeGutters) {
            gen.writeObjectField("opts", Collections.singletonList(ImmutableMap.of("type", "excludeGutters")));
        } else {
            gen.writeObjectField("opts", Collections.emptyList());
        }
        gen.writeBooleanField("withDefault", withDefault);
        gen.writeBooleanField("fromPredict", fromPredict);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        // TODO: Validate more List<String>s.... somehow.
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricRegroup)) {
            return false;
        }
        final MetricRegroup that = (MetricRegroup) o;
        return min == that.min &&
                max == that.max &&
                interval == that.interval &&
                excludeGutters == that.excludeGutters &&
                withDefault == that.withDefault &&
                fromPredict == that.fromPredict &&
                Objects.equal(perDatasetMetric, that.perDatasetMetric);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(perDatasetMetric, min, max, interval, excludeGutters, withDefault, fromPredict);
    }

    @Override
    public String toString() {
        return "MetricRegroup{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", min=" + min +
                ", max=" + max +
                ", interval=" + interval +
                ", excludeGutters=" + excludeGutters +
                ", withDefault=" + withDefault +
                ", fromPredict=" + fromPredict +
                '}';
    }
}
