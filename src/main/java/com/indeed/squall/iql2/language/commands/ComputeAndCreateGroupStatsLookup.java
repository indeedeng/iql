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
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ComputeAndCreateGroupStatsLookup implements Command, JsonSerializable {
    public final Command computation;
    public final Optional<String> name;

    public ComputeAndCreateGroupStatsLookup(Command computation, Optional<String> name) {
        this.computation = computation;
        this.name = name;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "computeAndCreateGroupStatsLookup");
        gen.writeObjectField("computation", computation);
        gen.writeStringField("name", name.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        computation.validate(validationHelper, validator);
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.ComputeAndCreateGroupStatsLookup(
                computation.toExecutionCommand(namedMetricLookup, groupKeySet, options),
                name
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeAndCreateGroupStatsLookup that = (ComputeAndCreateGroupStatsLookup) o;
        return Objects.equals(computation, that.computation) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(computation, name);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookup{" +
                "computation=" + computation +
                ", name=" + name +
                '}';
    }
}
