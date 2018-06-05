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
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TimePeriodRegroup implements Command, JsonSerializable {
    private final long periodMillis;
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;
    private final boolean isRelative;

    public TimePeriodRegroup(long periodMillis, Optional<String> timeField, Optional<String> timeFormat, boolean isRelative) {
        this.periodMillis = periodMillis;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.isRelative = isRelative;
    }


    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "timePeriodRegroup");
        gen.writeBooleanField("isRelative", isRelative);
        gen.writeNumberField("periodMillis", periodMillis);
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
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.TimePeriodRegroup(
                periodMillis,
                timeField,
                timeFormat,
                isRelative
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePeriodRegroup that = (TimePeriodRegroup) o;
        return Objects.equals(periodMillis, that.periodMillis) &&
                Objects.equals(timeField, that.timeField) &&
                Objects.equals(timeFormat, that.timeFormat) &&
                Objects.equals(isRelative, that.isRelative);
    }

    @Override
    public int hashCode() {
        return Objects.hash(periodMillis, timeField, timeFormat, isRelative);
    }

    @Override
    public String toString() {
        return "TimePeriodRegroup{" +
                "periodMillis=" + periodMillis +
                ", timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                ", isRelative=" + isRelative +
                '}';
    }

}
