package com.indeed.iql2.language.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringOrActionTest {
    @Test
    public void toStringIsntGiantTest() {
        final StringOrAction action = new StringOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.copyOf(LongStream.range(0, 10000).mapToObj(String::valueOf).collect(Collectors.toSet())),
                1, 1, 0
        );
        assertTrue(action.toString().length() < 1000);
    }

    @Test
    public void testToStringDifference() {
        final StringOrAction action1 = new StringOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of("1", "2", "3"),
                1, 1, 0
        );
        final StringOrAction action2 = new StringOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of("1"),
                1, 1, 0
        );
        final StringOrAction action3 = new StringOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of("1", "2", "4"),
                1, 1, 0
        );
        assertFalse("toString results must differ", action1.toString().equals(action2.toString()));
        assertFalse("toString results must differ", action2.toString().equals(action3.toString()));
        assertFalse("toString results must differ", action1.toString().equals(action3.toString()));
    }
}