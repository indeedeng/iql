package com.indeed.imhotep.ez;

/**
* @author jwolfe
*/
public interface StatReference {
    public double[] getGroupStats();

    public boolean isValid();
    public void invalidate();

    public double getValue(long[] stats);
}
