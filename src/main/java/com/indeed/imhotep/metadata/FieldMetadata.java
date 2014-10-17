/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.metadata;

import com.google.common.base.Strings;
import org.codehaus.jackson.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author vladimir
 */

public class FieldMetadata {
    @Nonnull final String name;
    @Nullable String friendlyName;
    @Nullable String description;
    @Nonnull FieldType type;
    @Nonnull final FieldType imhotepType;
    boolean isHidden;

    public FieldMetadata(@Nonnull String name, @Nonnull FieldType imhotepType) {
        this.name = name;
        this.imhotepType = imhotepType;
        this.type = imhotepType;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public String getFriendlyName() {
        return friendlyName;
    }

    public String getDescription() {
        return description;
    }

    @Nonnull
    public FieldType getImhotepType() {
        return imhotepType;
    }

    @Nonnull
    public FieldType getType() {
        return type;
    }

    public void setType(@Nonnull FieldType type) {
        this.type = type;
    }

    public void setFriendlyName(@Nullable String friendlyName) {
        this.friendlyName = friendlyName;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

    public boolean isIntImhotepField() {
        return imhotepType == FieldType.Integer;
    }

    public boolean isStringImhotepField() {
        return imhotepType == FieldType.String;
    }

    public void toJSON(@Nonnull ObjectNode jsonNode) {
        jsonNode.put("name", getName());
        final String description = Strings.nullToEmpty(getDescription());
        jsonNode.put("description", description);
        jsonNode.put("type", getType().toString());
        if(getType() != getImhotepType()) {
            jsonNode.put("imhotepType", getImhotepType().toString());
        }
    }
}
