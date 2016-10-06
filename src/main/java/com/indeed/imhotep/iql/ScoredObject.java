package com.indeed.imhotep.iql;

import com.google.common.primitives.Doubles;

import java.util.Comparator;

/**
 * @author jsadun
 */
public final class ScoredObject<T> {
    // do a custom comparator to ensure that real numbers are preferred to NaNs
    public static final Comparator<ScoredObject> TOP_SCORE_COMPARATOR = new Comparator<ScoredObject>() {
        @Override
        public int compare(final ScoredObject o1, final ScoredObject o2) {
            double score1 = o1.getScore();
            if(Double.isNaN(score1)) {
                score1 = Double.NEGATIVE_INFINITY;
            }
            double score2 = o2.getScore();
            if(Double.isNaN(score2)) {
                score2 = Double.NEGATIVE_INFINITY;
            }
            return Doubles.compare(score1, score2);
        }
    };

    public static final Comparator<ScoredObject> BOTTOM_SCORE_COMPARATOR = new Comparator<ScoredObject>() {
        @Override
        public int compare(final ScoredObject o1, final ScoredObject o2) {
            double score1 = o1.getScore();
            if(Double.isNaN(score1)) {
                score1 = Double.POSITIVE_INFINITY;
            }
            double score2 = o2.getScore();
            if(Double.isNaN(score2)) {
                score2 = Double.POSITIVE_INFINITY;
            }
            // reverse the result
            return -Doubles.compare(score1, score2);
        }
    };

    private final double score;
    private final T object;

    public ScoredObject(final double score, final T object) {
        this.score = score;
        this.object = object;
    }

    public double getScore() {
        return score;
    }

    public T getObject() {
        return object;
    }
}
