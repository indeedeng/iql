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

        assertEquals(topComparator.compare(o1, o2), -1);
        assertEquals(bottomComparator.compare(o1, o2),1);
        assertEquals(topComparator.compare(o2, o3), 0);
        assertEquals(bottomComparator.compare(o2, o3), 0);
        assertEquals(topComparator.compare(o1, o4), -1);
        assertEquals(topComparator.compare(o5, o2), 1);
        assertEquals(bottomComparator.compare(o5, o2), -1);
        assertEquals(bottomComparator.compare(o6, o2), -1);
        assertEquals(topComparator.compare(o6, o2), -1);
        assertEquals(bottomComparator.compare(o7, o6), 1);
        assertEquals(topComparator.compare(o7, o6), 1);

    }

    @Test
    public void testIntComparator() {
        ScoredObject<Integer> o1 = new ScoredObject<>(123, new Integer(12));
        ScoredObject<Integer> o2 = new ScoredObject<>(123, new Integer(13));
        ScoredObject<Integer> o3 = new ScoredObject<>(123, new Integer(13));
        ScoredObject<Integer> o4 = new ScoredObject<>(124, new Integer(12));
        ScoredObject<Integer> o5 = new ScoredObject<>(124, new Integer(13));
        ScoredObject<Integer> o6 = new ScoredObject<>(Double.NaN, new Integer(13));
        ScoredObject<Integer> o7 = new ScoredObject<>(-12, new Integer(13));

        Comparator<ScoredObject<Integer>> topComparator =  ScoredObject.topScoredObjectComparator(Comparator.<Integer>naturalOrder());
        Comparator<ScoredObject<Integer>> bottomComparator =  ScoredObject.bottomScoredObjectComparator(Comparator.<Integer>naturalOrder());

        assertEquals(topComparator.compare(o1, o2), -1);
        assertEquals(bottomComparator.compare(o1, o2),1);
        assertEquals(topComparator.compare(o2, o3), 0);
        assertEquals(bottomComparator.compare(o2, o3), 0);
        assertEquals(topComparator.compare(o1, o4), -1);
        assertEquals(topComparator.compare(o5, o2), 1);
        assertEquals(bottomComparator.compare(o5, o2), -1);
        assertEquals(bottomComparator.compare(o6, o2), -1);
        assertEquals(topComparator.compare(o6, o2), -1);
        assertEquals(bottomComparator.compare(o7, o6), 1);
        assertEquals(topComparator.compare(o7, o6), 1);

    }
}
