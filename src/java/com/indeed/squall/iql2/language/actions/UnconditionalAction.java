package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UnconditionalAction implements Action, JsonSerializable {
    public final ImmutableSet<String> scope;
    public final int targetGroup;
    public final int newGroup;

    public UnconditionalAction(Set<String> scope, int targetGroup, int newGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.targetGroup = targetGroup;
        this.newGroup = newGroup;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "unconditionalAction");
        m.put("scope", scope);
        m.put("target", targetGroup);
        m.put("newGroup", newGroup);
        gen.writeObject(m);
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

    }

    @Override
    public String toString() {
        return "UnconditionalAction{" +
                "scope=" + scope +
                ", targetGroup=" + targetGroup +
                ", newGroup=" + newGroup +
                '}';
    }
}
