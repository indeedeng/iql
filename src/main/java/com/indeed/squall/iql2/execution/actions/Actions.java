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

package com.indeed.squall.iql2.execution.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.QueryType;
import com.indeed.flamdex.query.Term;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Actions {
    public static Action parseFrom(JsonNode json) {
        final String actionType = json.get("action").textValue();
        final Set<String> scope = json.has("scope") ? readStringSet(json.get("scope")) : null;
        final Integer target = json.has("target") ? json.get("target").intValue() : null;
        final Integer positive = json.has("positive") ? json.get("positive").intValue() : null;
        final Integer negative = json.has("negative") ? json.get("negative").intValue() : null;
        switch (actionType) {
            case "intOrAction": {
                final Set<Long> terms = readLongSet(json.get("terms"));
                return new IntOrAction(scope, json.get("field").textValue(), terms, target, positive, negative);
            }
            case "stringOrAction": {
                final Set<String> terms = readStringSet(json.get("terms"));
                return new StringOrAction(scope, json.get("field").textValue(), terms, target, positive, negative);
            }
            case "metricAction": {
                final Map<String, List<String>> perDatasetFilterMetric = Maps.newHashMap();
                final JsonNode filters = json.get("perDatasetFilter");
                final Iterator<String> filterNameIterator = filters.fieldNames();
                while (filterNameIterator.hasNext()) {
                    final String filterName = filterNameIterator.next();
                    final List<String> pushes = Lists.newArrayList();
                    final JsonNode filterList = filters.get(filterName);
                    for (int i = 0; i < filterList.size(); i++) {
                        pushes.add(filterList.get(i).textValue());
                    }
                    perDatasetFilterMetric.put(filterName, pushes);
                }
                return new MetricAction(scope, perDatasetFilterMetric, target, positive, negative);
            }
            case "queryAction": {
                final Map<String, Query> perDatasetQueries = Maps.newHashMap();
                final JsonNode queries = json.get("perDatasetQuery");
                final Iterator<String> datasetIterator = queries.fieldNames();
                while (datasetIterator.hasNext()) {
                    final String dataset = datasetIterator.next();
                    perDatasetQueries.put(dataset, parseQuery(queries.get(dataset)));
                }
                return new QueryAction(scope, perDatasetQueries, target, positive, negative);
            }
            case "regexAction": {
                return new RegexAction(
                        scope,
                        json.get("field").textValue(),
                        json.get("regex").textValue(),
                        target,
                        positive,
                        negative
                );
            }
            case "sampleAction": {
                return new SampleAction(
                        scope,
                        json.get("field").textValue(),
                        json.get("probability").doubleValue(),
                        json.get("seed").textValue(),
                        target,
                        positive,
                        negative
                );
            }
            case "unconditionalAction": {
                return new UnconditionalAction(scope, target, json.get("newGroup").intValue());
            }
            default: {
                throw new IllegalArgumentException("Unhandled action type: " + actionType);
            }
        }
    }

    static Query parseQuery(JsonNode json) {
        final QueryType queryType = QueryType.valueOf(json.get("type").textValue());
        switch (queryType) {
            case TERM: {
                final Term startTerm = parseTerm(json.get("startTerm"));
                return Query.newTermQuery(startTerm);
            }
            case BOOLEAN: {
                final BooleanOp operator = BooleanOp.valueOf(json.get("operator").textValue());
                final List<Query> operands = new ArrayList<>();
                for (final JsonNode operand : json.get("operands")) {
                    operands.add(parseQuery(operand));
                }
                return Query.newBooleanQuery(operator, operands);
            }
            case RANGE: {
                final Term startTerm = parseTerm(json.get("startTerm"));
                final Term endTerm = parseTerm(json.get("endTerm"));
                final boolean isMaxInclusive = json.get("isMaxInclusive").booleanValue();
                return Query.newRangeQuery(startTerm, endTerm, isMaxInclusive);
            }
            default: {
                throw new IllegalArgumentException("Unrecognized query type: " + queryType);
            }
        }
    }

    static Term parseTerm(JsonNode json) {
        final String field = json.get("field").textValue();
        final boolean isIntField = json.get("isIntField").booleanValue();
        if (isIntField) {
            return Term.intTerm(field, json.get("intTerm").longValue());
        } else {
            return Term.stringTerm(field, json.get("stringTerm").textValue());
        }
    }

    static Set<String> readStringSet(JsonNode jsonNode) {
        final Set<String> result = new HashSet<>(jsonNode.size());
        for (int i = 0; i < jsonNode.size(); i++) {
            result.add(jsonNode.get(i).textValue());
        }
        return result;
    }

    static Set<Long> readLongSet(JsonNode jsonNode) {
        final Set<Long> result = new HashSet<>(jsonNode.size());
        for (int i = 0; i < jsonNode.size(); i++) {
            result.add(jsonNode.get(i).longValue());
        }
        return result;
    }
}
