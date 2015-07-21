package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.Objects;

public class RegroupIntoParent implements Command, JsonSerializable {
    private final GroupLookupMergeType mergeType;

    public RegroupIntoParent(GroupLookupMergeType mergeType) {
        this.mergeType = mergeType;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(ImmutableMap.of("command", "regroupIntoParent", "mergeType", mergeType));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegroupIntoParent that = (RegroupIntoParent) o;
        return Objects.equals(mergeType, that.mergeType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergeType);
    }

    @Override
    public String toString() {
        return "RegroupIntoParent{" +
                "mergeType=" + mergeType +
                '}';
    }
}
