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
 package com.indeed.iql.metadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author vladimir
 */
public class MetricMetadata {
    public @Nonnull final String name;
    public @Nullable String fieldAlias;
    public @Nullable String friendlyName;
    public @Nullable String description;
    public @Nullable String expression;
    public @Nullable String unit;
    public boolean isHidden = false;

    // IQL2 only
    public boolean isAlias;

    // TODO delete
    public MetricMetadata(final String name, final String expression, final String description, final @Nullable String fieldAlias) {
        this.name = name;
        this.expression = expression;
        this.description = description;
        this.fieldAlias = fieldAlias;
        this.isAlias = fieldAlias != null;
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

    public Optional<String> getAliasActualField() {
        return Optional.fromNullable(fieldAlias);
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
