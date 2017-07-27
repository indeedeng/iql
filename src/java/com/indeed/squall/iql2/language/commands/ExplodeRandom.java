package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Objects;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;

public class ExplodeRandom implements Command, JsonSerializable {
    private final String field;
    private final int k;
    private final String salt;

    public ExplodeRandom(String field, int k, String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeRandom");
        gen.writeStringField("field", field);
        gen.writeNumberField("k", k);
        gen.writeStringField("salt", salt);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
        this.serialize(jsonGenerator, serializerProvider);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodeRandom that = (ExplodeRandom) o;
        return k == that.k &&
                Objects.equal(field, that.field) &&
                Objects.equal(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field, k, salt);
    }

    @Override
    public String toString() {
        return "ExplodeRandom{" +
                "field='" + field + '\'' +
                ", k=" + k +
                ", salt='" + salt + '\'' +
                '}';
    }
}
