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
