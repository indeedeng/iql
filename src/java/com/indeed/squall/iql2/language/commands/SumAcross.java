package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class SumAcross implements Command, JsonSerializable {
    public final Set<String> scope;
    public final String field;
    public final AggregateMetric metric;
    public final Optional<AggregateFilter> filter;

    public SumAcross(Set<String> scope, String field, AggregateMetric metric, Optional<AggregateFilter> filter) {
        this.scope = scope;
        this.field = field;
        this.metric = metric;
        this.filter = filter;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "sumAcross");
        gen.writeObjectField("scope", scope);
        gen.writeStringField("field", field);
        gen.writeObjectField("metric", metric);
        gen.writeObjectField("filter", filter.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumAcross sumAcross = (SumAcross) o;
        return Objects.equals(scope, sumAcross.scope) &&
                Objects.equals(field, sumAcross.field) &&
                Objects.equals(metric, sumAcross.metric) &&
                Objects.equals(filter, sumAcross.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, field, metric, filter);
    }

    @Override
    public String toString() {
        return "SumAcross{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", metric=" + metric +
                ", filter=" + filter +
                '}';
    }
}
