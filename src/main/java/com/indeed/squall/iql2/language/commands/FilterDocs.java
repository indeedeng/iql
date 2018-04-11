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
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FilterDocs implements Command, JsonSerializable {
    public final ImmutableMap<String, DocMetric.PushableDocMetric> perDatasetFilterMetric;

    public FilterDocs(Map<String, DocMetric.PushableDocMetric> perDatasetFilterMetric) {
        this.perDatasetFilterMetric = ImmutableMap.copyOf(perDatasetFilterMetric);
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "filterDocs");
        gen.writeObjectField("perDatasetFilter", getPushesMap());
        gen.writeEndObject();
    }

    private Map<String, List<String>> getPushesMap() {
        final Map<String, List<String>> result = new HashMap<>();
        for (final Map.Entry<String, DocMetric.PushableDocMetric> entry : perDatasetFilterMetric.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getPushes(entry.getKey()));
        }
        return result;
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final Map.Entry<String, DocMetric.PushableDocMetric> entry : perDatasetFilterMetric.entrySet()) {
            entry.getValue().validate(entry.getKey(), validationHelper, validator);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterDocs that = (FilterDocs) o;
        return Objects.equals(perDatasetFilterMetric, that.perDatasetFilterMetric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(perDatasetFilterMetric);
    }

    @Override
    public String toString() {
        return "FilterDocs{" +
                "perDatasetFilterMetric=" + perDatasetFilterMetric +
                '}';
    }
}
