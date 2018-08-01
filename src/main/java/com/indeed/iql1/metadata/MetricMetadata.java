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
 package com.indeed.iql1.metadata;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author vladimir
 */
public class MetricMetadata {
    public @Nonnull final String name;
    public @Nullable String friendlyName;
    public @Nullable String description;
    public @Nullable String expression;
    public @Nullable String unit;
    public boolean isHidden = false;

    // IQL2 only
    public AggregateMetric metric;
    public boolean isAlias;

    public MetricMetadata(@Nonnull String name) {
        Preconditions.checkNotNull(name);
        this.name = name;
    }

    // TODO delete
    public MetricMetadata(final String name, final String expression, final String description, final AggregateMetric metric) {
        this.name = name;
        this.expression = expression;
        this.description = description;
        this.metric = metric;
        this.isAlias = isAliasDimension(metric);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nullable
    public String getFriendlyName() {
        return friendlyName;
    }

    public void setFriendlyName(@Nullable String friendlyName) {
        this.friendlyName = friendlyName;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    @Nullable
    public String getExpression() {
        return expression;
    }

    public void setExpression(@Nullable String expression) {
        this.expression = expression;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

    public AggregateMetric getMetric() {
        return metric;
    }

    public void setMetric(AggregateMetric metric) {
        this.metric = metric;
    }

    public boolean isAlias() {
        return isAlias;
    }

    public void setAlias(boolean alias) {
        isAlias = alias;
    }

    /**
     * Returns a unit of measurement for the metric (e.g. clicks, hours, etc.)
     */
    @Nullable
    public String getUnit() {
        return unit;
    }

    public void setUnit(@Nullable String unit) {
        this.unit = unit;
    }


    private boolean isAliasDimension(AggregateMetric metric) {
        return getAliasActualField().isPresent();
    }

    public Optional<String> getAliasActualField() {
        if ((metric instanceof AggregateMetric.DocStats)
                && (((AggregateMetric.DocStats) metric).docMetric instanceof DocMetric.Field)) {
            return Optional.of(((DocMetric.Field) ((AggregateMetric.DocStats) metric).docMetric).field);
        } else {
            return Optional.absent();
        }
    }

    public void toJSON(ObjectNode jsonNode) {
        jsonNode.put("name", getName());
        final String description = Strings.nullToEmpty(getDescription());
        jsonNode.put("description", description);
        String unit = getUnit();
        if(unit != null) {
            jsonNode.put("unit", unit);
        }
        final String expression = getExpression();
        if(expression != null) {
            jsonNode.put("expression", expression);
        }
    }
}
