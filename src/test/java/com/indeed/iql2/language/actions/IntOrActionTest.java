package com.indeed.iql2.language.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class IntOrActionTest {
    @Test
    public void toStringIsntGiantTest() {
        final IntOrAction action = new IntOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.copyOf(LongStream.range(0, 10000).boxed().collect(Collectors.toSet())),
                1, 1, 0
        );
        assertTrue(action.toString().length() < 1000);
    }

    @Test
    public void testToStringDifference() {
        final IntOrAction action1 = new IntOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of(1L, 2L, 3L),
                1, 1, 0
        );
        final IntOrAction action2 = new IntOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of(1L),
                1, 1, 0
        );
        final IntOrAction action3 = new IntOrAction(
                FieldSet.of("foo", "bar", true),
                ImmutableSet.of(1L, 2L, 4L),
                1, 1, 0
        );
        assertFalse("toString results must differ", action1.toString().equals(action2.toString()));
        assertFalse("toString results must differ", action2.toString().equals(action3.toString()));
        assertFalse("toString results must differ", action1.toString().equals(action3.toString()));
    }
}