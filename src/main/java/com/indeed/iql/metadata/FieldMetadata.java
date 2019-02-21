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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author vladimir
 */

public class FieldMetadata {
    @Nonnull final String name;
    @Nullable String description;
    @Nonnull
    FieldType type;
    @Nonnull
    List<String> aliases;   // first entry is the canonical name by convention
    int frequency;
    boolean isHidden;
    boolean isCertified;

    public FieldMetadata(@Nonnull String name, @Nonnull FieldType type) {
        this.name = name;
        this.type = type;
        aliases = Lists.newArrayList();
    }

    /** True field name as known to Imhotep */
    @Nonnull
    public String getName() {
        return name;
    }

    /** Name with canonical alias applied if it exists */
    @Nonnull
    public String getCanonicalName() {
        if(aliases.isEmpty()) {
            return name;
        } else {
            return aliases.get(0);
        }
    }

    public String getDescription() {
        return description;
    }

    @Nonnull
    public FieldType getType() {
        return type;
    }

    public FieldMetadata setType(@Nonnull FieldType type) {
        this.type = type;
        return this;
    }

    public FieldMetadata setDescription(@Nullable String description) {
        this.description = description;
        return this;
    }

    /** Aliases of this field other than the canonical name */
    @Nonnull
    public List<String> getNonCanonicalNames() {
        if (aliases.isEmpty()) {
            return Collections.emptyList();
        } else {
            final List<String> nonCanonicalNames = Lists.newArrayList(aliases);
            if (!name.equals(nonCanonicalNames.get(0))) {
                nonCanonicalNames.set(0, name);
            } else {
                nonCanonicalNames.remove(0);    // the true name is actually the canonical name so avoid duplication
            }
            return nonCanonicalNames;
        }
    }

    public List<String> getAliases() {
        return aliases;
    }

    public void setAliases(String aliases) {
        if(!Strings.isNullOrEmpty(aliases)) {
            // either space or comma can be use as names separator in IMS
            this.aliases.addAll(Arrays.asList(StringUtils.split(aliases, " ,")));
        }
    }

    public boolean isHidden() {
        return isHidden;
    }

    public FieldMetadata setHidden(boolean hidden) {
        isHidden = hidden;
        return this;
    }

    public boolean isCertified() {
        return isCertified;
    }

    public void setCertified(boolean certified) {
        this.isCertified = certified;
    }

    public int getFrequency() {
        return frequency;
    }

    public FieldMetadata setFrequency(final int frequency) {
        this.frequency = frequency;
        return this;
    }

    public boolean isIntField() {
        return type == FieldType.Integer;
    }

    public boolean isStringField() {
        return type == FieldType.String;
    }

    public void toJSON(@Nonnull ObjectNode jsonNode) {
        jsonNode.put("name", getCanonicalName());
        final String description = getAugmentedDescription();
        jsonNode.put("description", description);
        jsonNode.put("type", getType().toString());
        jsonNode.put("frequency", getFrequency());
        if (!getAliases().isEmpty()) {
            final ArrayNode aliasesArray = jsonNode.putArray("aliases");
            for (String alias : getNonCanonicalNames()) {
                aliasesArray.add(alias);
            }
        }
        if (isCertified) {
            jsonNode.put("certified", isCertified);
        }
    }

    private String getAugmentedDescription() {
        if (aliases.isEmpty()) {
            return Strings.nullToEmpty(getDescription());
        }

        final String aliasesDescription = "(aliases: " + StringUtils.join(getNonCanonicalNames(), ", ") + ")";
        if (Strings.isNullOrEmpty(description)) {
            return aliasesDescription;
        } else {
            return description + " " + aliasesDescription;
        }
    }

    public String toTSV() {
        final String description = getAugmentedDescription();
        return getCanonicalName() + "\t" + description.replaceAll("[\r\n\t]+", " ");
    }

    public static final Comparator<FieldMetadata> COMPARATOR = new CaseSensitiveComparator();

    public static final class CaseSensitiveComparator implements Comparator<FieldMetadata> {
        public int compare(FieldMetadata f1, FieldMetadata f2) {
            String s1 = f1.getName();
            String s2 = f2.getName();
            return s1.compareTo(s2);
        }
    }
}
