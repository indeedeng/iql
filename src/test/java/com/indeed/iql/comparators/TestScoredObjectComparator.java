package com.indeed.iql.comparators;

import com.indeed.iql1.iql.ScoredObject;
import org.junit.Test;
import java.util.Comparator;

import static org.junit.Assert.*;

public class TestScoredObjectComparator {

    @Test
    public void testStrComparator() {
        ScoredObject o1 = new ScoredObject(123, new String("a"));
        ScoredObject o2 = new ScoredObject(123, new String("b"));

        Comparator<ScoredObject<String>> topComparator =  ScoredObject.topScoredObjectComparator(Comparator.<String>naturalOrder());
        Comparator<ScoredObject<String>> bottomComparator =  ScoredObject.bottomScoredObjectComparator(Comparator.<String>naturalOrder());

        assertEquals(topComparator.compare(o1,o2), -1);
        assertEquals(bottomComparator.compare(o1,o2),1);
    }

    @Test
    public void testIntComparator() {
        ScoredObject o1 = new ScoredObject(123, new Integer(12));
        ScoredObject o2 = new ScoredObject(123, new Integer(13));

        Comparator<ScoredObject<Integer>> topComparator =  ScoredObject.topScoredObjectComparator(Comparator.<Integer>naturalOrder());
        Comparator<ScoredObject<Integer>> bottomComparator =  ScoredObject.bottomScoredObjectComparator(Comparator.<Integer>naturalOrder());

        assertEquals(topComparator.compare(o1,o2), -1);
        assertEquals(bottomComparator.compare(o1,o2),1);
    }
}
