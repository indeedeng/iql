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
import com.google.common.base.Objects;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.List;

public class ExplodeRandom implements Command, JsonSerializable {
    private final String field;
    private final int k;
    private final String salt;

    public ExplodeRandom(String field, int k, String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final String dataset : validationHelper.datasets()) {
            validationHelper.containsField(dataset, field);
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.ExplodeRandom(
                field,
                k,
                salt
        );
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeRandom");
        gen.writeStringField("field", field);
        gen.writeNumberField("k", k);
        gen.writeStringField("salt", salt);
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
        ExplodeRandom that = (ExplodeRandom) o;
        return k == that.k &&
                Objects.equal(field, that.field) &&
                Objects.equal(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field, k, salt);
    }

    @Override
    public String toString() {
        return "ExplodeRandom{" +
                "field='" + field + '\'' +
                ", k=" + k +
                ", salt='" + salt + '\'' +
                '}';
    }
}
