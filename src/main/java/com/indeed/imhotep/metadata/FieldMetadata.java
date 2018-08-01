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
 package com.indeed.imhotep.metadata;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * @author vladimir
 */

public class FieldMetadata {
    @Nonnull final String name;
    @Nullable String friendlyName;
    @Nullable String description;
    @Nonnull FieldType type;
    int frequency;
    boolean isHidden;

    public FieldMetadata(@Nonnull String name, @Nonnull FieldType type) {
        this.name = name;
        this.type = type;
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

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(final int frequency) {
        this.frequency = frequency;
    }

    public boolean isIntImhotepField() {
        return type == FieldType.Integer;
    }

    public boolean isStringImhotepField() {
        return type == FieldType.String;
    }

    public void toJSON(@Nonnull ObjectNode jsonNode) {
        jsonNode.put("name", getName());
        final String description = Strings.nullToEmpty(getDescription());
        jsonNode.put("description", description);
        jsonNode.put("type", getType().toString());
        jsonNode.put("frequency", getFrequency());

    }

    public static final Comparator<FieldMetadata> CASE_INSENSITIVE_ORDER = new CaseInsensitiveComparator();

    public static final class CaseInsensitiveComparator implements Comparator<FieldMetadata> {
        public int compare(FieldMetadata f1, FieldMetadata f2) {
            String s1 = f1.getName();
            String s2 = f2.getName();
            return s1.compareToIgnoreCase(s2);
        }
    }
}
