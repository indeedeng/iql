package com.indeed.imhotep.ez;

/**
* @author jwolfe
*/
public class SingleStatReference implements StatReference {
    final int depth;
    private String stringRep;
    private final EZImhotepSession session;
    boolean valid = true;

    SingleStatReference(int depth, String stringRep, EZImhotepSession session) {
        this.depth = depth;
        this.stringRep = stringRep;
        this.session = session;
    }

    @Override
    public String toString() {
        if (valid) {
            return "reference("+stringRep+")";
        } else {
            return "invalid stat reference";
        }
    }

    @Override
    public double[] getGroupStats() {
        Stats.requireValid(this);
        long[] values = session.getGroupStats(depth);
        double[] realValues = new double[values.length];
        for(int i = 0; i < values.length; i++) {
            realValues[i] = values[i];
        }
        return realValues;
    }

    @Override
    public double getValue(long[] stats) {
        return (double)stats[depth];
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void invalidate() {
        valid = false;
    }
}
