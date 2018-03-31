package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RegroupFieldIn implements Command, JsonSerializable {
    private final Set<String> scope;
    private final String field;
    private final List<String> stringTerms;
    private final LongList intTerms;
    private final boolean isIntField;
    private final boolean withDefault;

    public RegroupFieldIn(Set<String> scope, String field, List<String> stringTerms, LongList intTerms, boolean isIntField, boolean withDefault) {
        this.scope = scope;
        this.field = field;
        this.stringTerms = stringTerms;
        this.intTerms = intTerms;
        this.isIntField = isIntField;
        this.withDefault = withDefault;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "regroupFieldIn");
        gen.writeStringField("field", field);
        gen.writeObjectField("stringTerms", stringTerms);
        gen.writeObjectField("intTerms", intTerms);
        gen.writeBooleanField("isIntField", isIntField);
        gen.writeBooleanField("withDefault", withDefault);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (isIntField) {
            ValidationUtil.validateIntField(scope, field, validationHelper, validator, this);
        } else {
            ValidationUtil.validateStringField(scope, field, validationHelper, validator, this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegroupFieldIn that = (RegroupFieldIn) o;
        return isIntField == that.isIntField &&
                withDefault == that.withDefault &&
                Objects.equals(scope, that.scope) &&
                Objects.equals(field, that.field) &&
                Objects.equals(stringTerms, that.stringTerms) &&
                Objects.equals(intTerms, that.intTerms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, field, stringTerms, intTerms, isIntField, withDefault);
    }

    @Override
    public String toString() {
        return "RegroupFieldIn{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", stringTerms=" + stringTerms +
                ", intTerms=" + intTerms +
                ", isIntField=" + isIntField +
                ", withDefault=" + withDefault +
                '}';
    }
}
