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

package com.indeed.squall.iql2.language.dimensions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;

public class Dimension {
    public final String name;
    public final String expression;
    public final String description;
    @JsonIgnore
    public final AggregateMetric metric;
    @JsonIgnore
    public final boolean isAlias;

    public Dimension(final String name, final String expression, final String description, final AggregateMetric metric) {
        this.name = name;
        this.expression = expression;
        this.description = description;
        this.metric = metric;
        this.isAlias = isAliasDimension(metric);
    }


    @JsonIgnore
    private boolean isAliasDimension(AggregateMetric metric) {
        return getAliasActualField().isPresent();
    }

    @JsonIgnore
    public Optional<String> getAliasActualField() {
        if ((metric instanceof AggregateMetric.DocStats)
                && (((AggregateMetric.DocStats) metric).docMetric instanceof DocMetric.Field)) {
            return Optional.of(((DocMetric.Field) ((AggregateMetric.DocStats) metric).docMetric).field);
        } else {
            return Optional.absent();
        }
    }
}
