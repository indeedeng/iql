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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SampleFields implements Command, JsonSerializable {
    private final Map<String, List<DocFilter.Sample>> perDatasetSamples;

    public SampleFields(Map<String, List<DocFilter.Sample>> perDatasetSamples) {
        this.perDatasetSamples = perDatasetSamples;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, List<Map<String, Object>>> perDatasetSamples = new HashMap<>();
        for (final Map.Entry<String, List<DocFilter.Sample>> entry : this.perDatasetSamples.entrySet()) {
            final List<Map<String, Object>> l = new ArrayList<>();
            for (final DocFilter.Sample sample : entry.getValue()) {
                l.add(ImmutableMap.<String, Object>of("field", sample.field.unwrap(), "fraction", ((double) sample.numerator) / sample.denominator, "seed", sample.seed));
            }
            perDatasetSamples.put(entry.getKey(), l);
        }
        gen.writeObject(ImmutableMap.of("command", "sampleFields", "perDatasetSamples", perDatasetSamples));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final Map.Entry<String, List<DocFilter.Sample>> entry : perDatasetSamples.entrySet()) {
            final String dataset = entry.getKey();
            for (final DocFilter.Sample sample : entry.getValue()) {
                ValidationUtil.validateField(ImmutableSet.of(dataset), sample.field.unwrap(), validationHelper, validator, this);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleFields that = (SampleFields) o;
        return Objects.equals(perDatasetSamples, that.perDatasetSamples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(perDatasetSamples);
    }

    @Override
    public String toString() {
        return "SampleFields{" +
                "perDatasetSamples=" + perDatasetSamples +
                '}';
    }
}
