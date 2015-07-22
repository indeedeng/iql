package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;

public class ExplodeByAggregatePercentile implements Command, JsonSerializable {
    public final String field;
    public final AggregateMetric metric;
    public final int numBuckets;

    public ExplodeByAggregatePercentile(String field, AggregateMetric metric, int numBuckets) {
        this.field = field;
        this.metric = metric;
        this.numBuckets = numBuckets;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeByAggregatePercentile");
        gen.writeStringField("field", field);
        gen.writeObjectField("metric", metric);
        gen.writeNumberField("numBuckets", numBuckets);
        gen.writeEndObject();
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

        metric.validate(datasetsFields.datasets(), datasetsFields, errorConsumer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodeByAggregatePercentile that = (ExplodeByAggregatePercentile) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(field, that.field) &&
                Objects.equals(metric, that.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metric, numBuckets);
    }

    @Override
    public String toString() {
        return "ExplodeByAggregatePercentile{" +
                "field='" + field + '\'' +
                ", metric=" + metric +
                ", numBuckets=" + numBuckets +
                '}';
    }
}
