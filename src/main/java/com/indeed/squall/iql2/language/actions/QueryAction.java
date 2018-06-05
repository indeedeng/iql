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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryAction implements Action, JsonSerializable {
    public final ImmutableSet<String> scope;
    public final Map<String, Query> perDatasetQuery;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public QueryAction(Set<String> scope, Map<String, Query> perDatasetQuery, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.perDatasetQuery = ImmutableMap.copyOf(perDatasetQuery);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "queryAction");
        m.put("scope", scope);
        final Map<String, JsonNode> serializedQueries = new HashMap<>();
        for (final Map.Entry<String, Query> entry : perDatasetQuery.entrySet()) {
            serializedQueries.put(entry.getKey(), Actions.queryJson(entry.getValue()));
        }
        m.put("perDatasetQuery", serializedQueries);
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
        ValidationUtil.validateQuery(validationHelper, perDatasetQuery, validator, this);
    }

    @Override
    public com.indeed.squall.iql2.execution.actions.Action toExecutionAction(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.squall.iql2.execution.actions.QueryAction(
                scope,
                perDatasetQuery,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }

    @Override
    public String toString() {
        return "QueryAction{" +
                "scope=" + scope +
                ", perDatasetQuery=" + perDatasetQuery +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
