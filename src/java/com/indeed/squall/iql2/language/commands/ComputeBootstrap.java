package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ComputeBootstrap implements Command, JsonSerializable {
    public final Set<String> scope;
    public final String field;
    private final Optional<AggregateFilter> filter;
    private final String seed;
    private final AggregateMetric metric;
    private final int numBootstraps;
    private final List<String> varargs;

    public ComputeBootstrap(Set<String> scope, String field, Optional<AggregateFilter> filter, String seed, AggregateMetric metric, int numBootstraps, List<String> varargs) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
        this.seed = seed;
        this.metric = metric;
        this.numBootstraps = numBootstraps;
        this.varargs = varargs;
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        ValidationUtil.validateField(scope, field, datasetsFields, validator, this);
        metric.validate(scope, datasetsFields, validator);
        if (filter.isPresent()) {
            filter.get().validate(scope, datasetsFields, validator);
        }
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "computeBootstrap");
        gen.writeObjectField("scope", scope);
        gen.writeStringField("field", field);
        gen.writeObjectField("filter", filter.orNull());
        gen.writeStringField("seed", seed);
        gen.writeObjectField("metric", metric);
        gen.writeNumberField("numBootstraps", numBootstraps);
        gen.writeObjectField("varargs", varargs);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
        this.serialize(jsonGenerator, serializerProvider);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeBootstrap that = (ComputeBootstrap) o;
        return numBootstraps == that.numBootstraps &&
                Objects.equal(scope, that.scope) &&
                Objects.equal(field, that.field) &&
                Objects.equal(filter, that.filter) &&
                Objects.equal(seed, that.seed) &&
                Objects.equal(metric, that.metric) &&
                Objects.equal(varargs, that.varargs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(scope, field, filter, seed, metric, numBootstraps, varargs);
    }

    @Override
    public String toString() {
        return "ComputeBootstrap{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", filter=" + filter +
                ", seed='" + seed + '\'' +
                ", metric=" + metric +
                ", numBootstraps=" + numBootstraps +
                ", varargs=" + varargs +
                '}';
    }
}
