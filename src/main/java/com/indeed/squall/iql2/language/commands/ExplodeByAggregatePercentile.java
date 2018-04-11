/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
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
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final String dataset : validationHelper.datasets()) {
            if (!validationHelper.containsField(dataset, field)) {
                validator.error(ErrorMessages.missingField(dataset, field, this));
            }
        }

        metric.validate(validationHelper.datasets(), validationHelper, validator);
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
