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
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.Objects;

public class ExplodeTimeBuckets implements Command, JsonSerializable {
    private final int numBuckets;
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;

    public ExplodeTimeBuckets(int numBuckets, Optional<String> timeField, Optional<String> timeFormat) {
        this.numBuckets = numBuckets;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeTimeBuckets");
        gen.writeNumberField("numBuckets", numBuckets);
        gen.writeStringField("timeField", timeField.orNull());
        gen.writeStringField("timeFormat", timeFormat.orNull());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (timeField.isPresent()) {
            ValidationUtil.validateIntField(validationHelper.datasets(), timeField.get(), validationHelper, validator, this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodeTimeBuckets that = (ExplodeTimeBuckets) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(timeField, that.timeField) &&
                Objects.equals(timeFormat, that.timeFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numBuckets, timeField, timeFormat);
    }

    @Override
    public String toString() {
        return "ExplodeTimeBuckets{" +
                "numBuckets=" + numBuckets +
                ", timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
