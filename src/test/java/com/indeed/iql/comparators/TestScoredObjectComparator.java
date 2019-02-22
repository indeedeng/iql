package com.indeed.iql.comparators;

import com.indeed.iql1.iql.ScoredObject;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class TestScoredObjectComparator {

    @Test
    public void testStrComparator() {
        ScoredObject<String> o1 = new ScoredObject<>(123, new String("a"));
        ScoredObject<String> o2 = new ScoredObject<>(123, new String("b"));
        ScoredObject<String> o3 = new ScoredObject<>(123, new String("b"));
        ScoredObject<String> o4 = new ScoredObject<>(124, new String("a"));
        ScoredObject<String> o5 = new ScoredObject<>(124, new String("b"));
        ScoredObject<String> o6 = new ScoredObject<>(Double.NaN, new String("b"));
        ScoredObject<String> o7 = new ScoredObject<>(-12, new String("b"));

        Comparator<ScoredObject<String>> topComparator =  ScoredObject.topScoredObjectComparator(Comparator.<String>naturalOrder());
        Comparator<ScoredObject<String>> bottomComparator =  ScoredObject.bottomScoredObjectComparator(Comparator.<String>naturalOrder());

        assertEquals(-1, topComparator.compare(o1, o2));
        assertEquals(1, bottomComparator.compare(o1, o2));
        assertEquals(0, topComparator.compare(o2, o3));
        assertEquals(0, bottomComparator.compare(o2, o3));
        assertEquals(-1, topComparator.compare(o1, o4));
        assertEquals(1, topComparator.compare(o5, o2));
        assertEquals(-1, bottomComparator.compare(o5, o2));
        assertEquals(-1, bottomComparator.compare(o6, o2));
        assertEquals(-1, topComparator.compare(o6, o2));
        assertEquals(1, bottomComparator.compare(o7, o6));
        assertEquals(1, topComparator.compare(o7, o6));

    }

    @Test
    public void testIntComparator() {
        ScoredObject<Integer> o1 = new ScoredObject<>(123, 12);
        ScoredObject<Integer> o2 = new ScoredObject<>(123, 13);
        ScoredObject<Integer> o3 = new ScoredObject<>(123, 13);
        ScoredObject<Integer> o4 = new ScoredObject<>(124, 12);
        ScoredObject<Integer> o5 = new ScoredObject<>(124, 13);
        ScoredObject<Integer> o6 = new ScoredObject<>(Double.NaN, 13);
        ScoredObject<Integer> o7 = new ScoredObject<>(-12, 13);

        Comparator<ScoredObject<Integer>> topComparator =  ScoredObject.topScoredObjectComparator(Comparator.<Integer>naturalOrder());
        Comparator<ScoredObject<Integer>> bottomComparator =  ScoredObject.bottomScoredObjectComparator(Comparator.<Integer>naturalOrder());

        assertEquals(-1, topComparator.compare(o1, o2));
        assertEquals(1, bottomComparator.compare(o1, o2));
        assertEquals(0, topComparator.compare(o2, o3));
        assertEquals(0, bottomComparator.compare(o2, o3));
        assertEquals(-1, topComparator.compare(o1, o4));
        assertEquals(1, topComparator.compare(o5, o2));
        assertEquals(-1, bottomComparator.compare(o5, o2));
        assertEquals(-1, bottomComparator.compare(o6, o2));
        assertEquals(-1, topComparator.compare(o6, o2));
        assertEquals(1, bottomComparator.compare(o7, o6));
        assertEquals(1, topComparator.compare(o7, o6));

    }
}
