package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.io.IOException;
import java.util.Objects;

public class RegroupIntoLastSiblingWhere implements Command, JsonSerializable {
    private final AggregateFilter filter;
    private final GroupLookupMergeType mergeType;

    public RegroupIntoLastSiblingWhere(AggregateFilter filter, GroupLookupMergeType mergeType) {
        this.filter = filter;
        this.mergeType = mergeType;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(ImmutableMap.of("command", "regroupIntoLastSiblingWhere", "filter", filter, "mergeType", mergeType));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        filter.validate(validationHelper.datasets(), validationHelper, validator);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegroupIntoLastSiblingWhere that = (RegroupIntoLastSiblingWhere) o;
        return Objects.equals(filter, that.filter) &&
                Objects.equals(mergeType, that.mergeType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, mergeType);
    }

    @Override
    public String toString() {
        return "RegroupIntoLastSiblingWhere{" +
                "filter=" + filter +
                ", mergeType=" + mergeType +
                '}';
    }
}
