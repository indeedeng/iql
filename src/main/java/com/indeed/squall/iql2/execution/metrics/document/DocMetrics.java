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

package com.indeed.squall.iql2.execution.metrics.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.List;

public class DocMetrics {
    public static DocMetric fromJson(JsonNode node) {
        switch (node.get("type").textValue()) {
            case "docStats": {
                final JsonNode pushes = node.get("pushes");
                final List<String> statPushes = Lists.newArrayList();
                for (final JsonNode push : pushes) {
                    statPushes.add(push.textValue());
                }
                return new DocMetric.BaseMetric(statPushes);
            }
        }
        throw new RuntimeException("Oops: " + node);
    }
}
