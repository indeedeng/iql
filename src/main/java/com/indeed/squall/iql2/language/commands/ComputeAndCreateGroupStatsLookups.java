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
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ComputeAndCreateGroupStatsLookups implements Command, JsonSerializable {
    private final List<Pair<Command, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Command, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "computeAndCreateGroupStatsLookups");
        final List<List<Object>> computations = new ArrayList<>();
        for (final Pair<Command, String> pair : namedComputations) {
            computations.add(Lists.newArrayList(pair.getFirst(), pair.getSecond()));
        }
        gen.writeObjectField("computations", computations);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final Pair<Command, String> pair : namedComputations) {
            pair.getFirst().validate(validationHelper, validator);
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.ComputeAndCreateGroupStatsLookups(
                namedComputations
                .stream()
                .map(x -> Pair.of(
                        x.getFirst().toExecutionCommand(namedMetricLookup, groupKeySet, options),
                        x.getSecond()
                ))
                .collect(Collectors.toList())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeAndCreateGroupStatsLookups that = (ComputeAndCreateGroupStatsLookups) o;
        return Objects.equals(namedComputations, that.namedComputations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namedComputations);
    }

    @Override
    public String toString() {
        return "ComputeAndCreateGroupStatsLookups{" +
                "namedComputations=" + namedComputations +
                '}';
    }
}
