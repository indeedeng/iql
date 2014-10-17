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
 package com.indeed.imhotep.ez;

/**
* @author jwolfe
*/
public abstract class Field {
    protected final String fieldName;

    private Field(String fieldName) {
        this.fieldName = fieldName;
    }

    public static IntField intField(String fieldName) {
        return new IntField(fieldName);
    }

    public static StringField stringField(String fieldName) {
        return new StringField(fieldName);
    }

    public final String getFieldName() {
        return fieldName;
    }

    public abstract boolean isIntField();

    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Field field = (Field) o;

        if (!fieldName.equals(field.fieldName)) return false;

        return true;
    }

    public int hashCode() {
        return fieldName.hashCode();
    }

    public static final class IntField extends Field {

        public IntField(final String fieldName) {
            super(fieldName);
        }

        public boolean isIntField() {
            return true;
        }
    }

    public static final class StringField extends Field {

        public StringField(final String fieldName) {
            super(fieldName);
        }

        public boolean isIntField() {
            return false;
        }
    }
}
