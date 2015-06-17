package com.indeed.jql.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.jql.language.actions.Action;

import java.io.IOException;
import java.util.List;

public class ApplyFilterActions implements Command, JsonSerializable {
    public final ImmutableList<Action> actions;

    public ApplyFilterActions(List<Action> actions) {
        this.actions = ImmutableList.copyOf(actions);
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(ImmutableMap.of("command", "applyFilterActions", "actions", actions));
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "ApplyFilterActions{" +
                "actions=" + actions +
                '}';
    }
}
