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

package com.indeed.squall.iql2.language.metadata;

import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * @author yuanlei
 */

@Value.Style(allParameters = true, init = "set*")
@Value.Immutable
public abstract class FieldMetadata {
    public abstract String getName();
    @Nullable public abstract String getDescription();
    public abstract Type getType();
    public abstract int getFrequency();

    public enum Type {
        String,
        Integer
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

