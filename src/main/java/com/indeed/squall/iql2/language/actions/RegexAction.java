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

package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ErrorMessages;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RegexAction implements Action, JsonSerializable {
    public final ImmutableSet<String> scope;
    public final String field;
    public final String regex;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public RegexAction(Set<String> scope, String field, String regex, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        ValidationUtil.compileRegex(regex);
        this.regex = regex;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "regexAction");
        m.put("scope", scope);
        m.put("field", field);
        m.put("regex", regex);
        m.put("target", targetGroup);
        m.put("positive", positiveGroup);
        m.put("negative", negativeGroup);
        gen.writeObject(m);
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final String dataset : scope) {
            if (!validationHelper.containsStringField(dataset, field)) {
                validator.error(ErrorMessages.missingStringField(dataset, field, this));
            }
        }
    }

    @Override
    public String toString() {
        return "RegexAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", regex='" + regex + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
