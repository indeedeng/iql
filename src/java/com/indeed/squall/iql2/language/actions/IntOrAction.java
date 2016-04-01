package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IntOrAction implements Action, JsonSerializable {
    public final ImmutableSet<String> scope;
    public final String field;
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public IntOrAction(Set<String> scope, String field, Set<Long> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        final Map<String, Object> m = new HashMap<>();
        m.put("action", "intOrAction");
        m.put("scope", scope);
        m.put("field", field);
        m.put("terms", terms);
        m.put("target", targetGroup);
        m.put("positive", positiveGroup);
        m.put("negative", negativeGroup);
        gen.writeObject(m);
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Validator validator) {
        for (final String dataset : scope) {
            if (!datasetsFields.getAllFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingField(dataset, field, this));
            }
        }
    }

    @Override
    public String toString() {
        return "IntOrAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", terms=" + terms +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
