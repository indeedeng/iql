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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetGroupStats implements Command, JsonSerializable {
    public final List<AggregateMetric> metrics;
    public final List<Optional<String>> formatStrings;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, List<Optional<String>> formatStrings, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.formatStrings = formatStrings;
        this.returnGroupKeys = returnGroupKeys;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "getGroupStats");
        gen.writeObjectField("metrics", metrics);
        gen.writeArrayFieldStart("opts");
        if (returnGroupKeys) {
            gen.writeObject(ImmutableMap.of("type", "returnGroupKeys"));
        }
        gen.writeEndArray();
        gen.writeArrayFieldStart("formatStrings");
        for (final Optional<String> formatString : formatStrings) {
            if (formatString.isPresent()) {
                gen.writeString(formatString.get());
            } else {
                gen.writeNull();
            }
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final AggregateMetric metric : metrics) {
            metric.validate(validationHelper.datasets(), validationHelper, validator);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetGroupStats that = (GetGroupStats) o;
        return returnGroupKeys == that.returnGroupKeys &&
                Objects.equals(metrics, that.metrics) &&
                Objects.equals(formatStrings, that.formatStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, formatStrings, returnGroupKeys);
    }

    @Override
    public String toString() {
        return "GetGroupStats{" +
                "metrics=" + metrics +
                ", formatStrings=" + formatStrings +
                ", returnGroupKeys=" + returnGroupKeys +
                '}';
    }
}
