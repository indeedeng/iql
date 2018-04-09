package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Objects;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;

public class ExplodeRandomDocId implements Command, JsonSerializable {
    private final int k;
    private final String salt;

    public ExplodeRandomDocId(final int k, final String salt) {
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
    }

    @Override
    public void serialize(final JsonGenerator gen, final SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodeRandomDocId");
        gen.writeNumberField("k", k);
        gen.writeStringField("salt", salt);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(final JsonGenerator jsonGenerator,
                                  final SerializerProvider serializerProvider,
                                  final TypeSerializer typeSerializer) throws IOException {
        this.serialize(jsonGenerator, serializerProvider);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final ExplodeRandomDocId that = (ExplodeRandomDocId) o;
        return (k == that.k) && Objects.equal(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(k, salt);
    }

    @Override
    public String toString() {
        return "ExplodeRandomDocId{" +
                "k=" + k +
                ", salt='" + salt + '\'' +
                '}';
    }
}
