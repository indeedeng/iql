package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

import java.io.IOException;
import java.util.Arrays;

/**
* @author jwolfe
*/
public class TermSelects implements JsonSerializable {
    public final String field;

    public boolean isIntTerm;
    public String stringTerm;
    public long intTerm;

    public final double[] selects;
    public double topMetric;
    public final int group;

    public TermSelects(String field, boolean isIntTerm, String stringTerm, long intTerm, double[] selects, double topMetric, int group) {
        this.field = field;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
        this.selects = selects;
        this.topMetric = topMetric;
        this.group = group;
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider serializers) throws IOException {
        jgen.writeStartObject();
        jgen.writeObjectField("field", field);
        if (isIntTerm) {
            jgen.writeObjectField("intTerm", intTerm);
        } else {
            jgen.writeObjectField("stringTerm", stringTerm);
        }
        jgen.writeObjectField("selects", selects);
        jgen.writeNumberField("group", group);
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public String toString() {
        return "TermSelects{" +
                "isIntTerm=" + isIntTerm +
                ", stringTerm='" + stringTerm + '\'' +
                ", intTerm=" + intTerm +
                ", selects=" + Arrays.toString(selects) +
                ", topMetric=" + topMetric +
                ", group=" + group +
                '}';
    }
}
