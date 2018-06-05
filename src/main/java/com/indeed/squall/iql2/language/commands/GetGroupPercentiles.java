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
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class GetGroupPercentiles implements Command, JsonSerializable {
    public final Set<String> scope;
    public final String field;
    public final double[] percentiles;

    public GetGroupPercentiles(Set<String> scope, String field, double[] percentiles) {
        this.scope = scope;
        this.field = field;
        this.percentiles = percentiles;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "getGroupPercentiles");
        gen.writeObjectField("scope", scope);
        gen.writeStringField("field", field);
        gen.writeObjectField("percentiles", percentiles);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        ValidationUtil.validateIntField(scope, field, validationHelper, validator, this);
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.GetGroupPercentiles(scope, field, percentiles);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetGroupPercentiles that = (GetGroupPercentiles) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(field, that.field) &&
                Objects.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, field, percentiles);
    }

    @Override
    public String toString() {
        return "GetGroupPercentiles{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", percentiles=" + Arrays.toString(percentiles) +
                '}';
    }
}
