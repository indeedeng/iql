package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.util.Objects;

public class ExplodePerDocPercentile extends RequiresFTGSCommand {
    public final int numBuckets;

    public ExplodePerDocPercentile(String field, int numBuckets) {
        super(field);
        this.numBuckets = numBuckets;
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("command", "explodePerDocPercentile");
        gen.writeStringField("field", field);
        gen.writeNumberField("numBuckets", numBuckets);
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
        this.serialize(gen, serializers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodePerDocPercentile that = (ExplodePerDocPercentile) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, numBuckets);
    }

    @Override
    public String toString() {
        return "ExplodePerDocPercentile{" +
                "field='" + field + '\'' +
                ", numBuckets=" + numBuckets +
                '}';
    }
}
