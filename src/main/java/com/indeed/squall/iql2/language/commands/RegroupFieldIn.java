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
import com.indeed.squall.iql2.execution.commands.IntRegroupFieldIn;
import com.indeed.squall.iql2.execution.commands.StringRegroupFieldIn;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RegroupFieldIn implements Command, JsonSerializable {
    private final Set<String> scope;
    private final String field;
    private final List<String> stringTerms;
    private final LongList intTerms;
    private final boolean isIntField;
    private final boolean withDefault;

    public RegroupFieldIn(Set<String> scope, String field, List<String> stringTerms, LongList intTerms, boolean isIntField, boolean withDefault) {
        this.scope = scope;
        this.field = field;
        this.stringTerms = stringTerms;
        this.intTerms = intTerms;
        this.isIntField = isIntField;
        this.withDefault = withDefault;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "regroupFieldIn");
        gen.writeStringField("field", field);
        gen.writeObjectField("stringTerms", stringTerms);
        gen.writeObjectField("intTerms", intTerms);
        gen.writeBooleanField("isIntField", isIntField);
        gen.writeBooleanField("withDefault", withDefault);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (isIntField) {
            ValidationUtil.validateIntField(scope, field, validationHelper, validator, this);
        } else {
            ValidationUtil.validateStringField(scope, field, validationHelper, validator, this);
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        if (isIntField) {
            return new IntRegroupFieldIn(field, intTerms, withDefault);
        } else {
            return new StringRegroupFieldIn(field, stringTerms, withDefault);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegroupFieldIn that = (RegroupFieldIn) o;
        return isIntField == that.isIntField &&
                withDefault == that.withDefault &&
                Objects.equals(scope, that.scope) &&
                Objects.equals(field, that.field) &&
                Objects.equals(stringTerms, that.stringTerms) &&
                Objects.equals(intTerms, that.intTerms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, field, stringTerms, intTerms, isIntField, withDefault);
    }

    @Override
    public String toString() {
        return "RegroupFieldIn{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", stringTerms=" + stringTerms +
                ", intTerms=" + intTerms +
                ", isIntField=" + isIntField +
                ", withDefault=" + withDefault +
                '}';
    }
}
