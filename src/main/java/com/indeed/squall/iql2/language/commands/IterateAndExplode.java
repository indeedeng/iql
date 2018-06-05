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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IterateAndExplode implements Command, JsonSerializable {
    public final String field;
    public final FieldIterateOpts fieldOpts;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(String field, FieldIterateOpts fieldOpts, Optional<String> explodeDefaultName) {
        this.field = field;
        this.fieldOpts = fieldOpts;
        this.explodeDefaultName = explodeDefaultName;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "iterateAndExplode");
        gen.writeStringField("field", field);
        gen.writeArrayFieldStart("iterOpts");
        gen.writeObject(ImmutableMap.of("type", "defaultedFieldOpts", "opts", fieldOpts));
        gen.writeEndArray();
        gen.writeArrayFieldStart("explodeOpts");
        if (explodeDefaultName.isPresent()) {
            gen.writeObject(ImmutableMap.of("type", "addDefault", "name", explodeDefaultName.get()));
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
        ValidationUtil.validateField(validationHelper.datasets(), field, validationHelper, validator, this);

        if (fieldOpts.topK.isPresent()) {
            final TopK topK = fieldOpts.topK.get();
            if (topK.metric.isPresent()) {
                topK.metric.get().validate(validationHelper.datasets(), validationHelper, validator);
            }
        }

        if (fieldOpts.filter.isPresent()) {
            fieldOpts.filter.get().validate(validationHelper.datasets(), validationHelper, validator);
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.IterateAndExplode(
                field,
                Collections.emptyList(),
                fieldOpts.toExecution(namedMetricLookup, groupKeySet),
                explodeDefaultName,
                null
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterateAndExplode that = (IterateAndExplode) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(fieldOpts, that.fieldOpts) &&
                Objects.equals(explodeDefaultName, that.explodeDefaultName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, fieldOpts, explodeDefaultName);
    }

    @Override
    public String toString() {
        return "IterateAndExplode{" +
                "field='" + field + '\'' +
                ", fieldOpts=" + fieldOpts +
                ", explodeDefaultName=" + explodeDefaultName +
                '}';
    }
}
