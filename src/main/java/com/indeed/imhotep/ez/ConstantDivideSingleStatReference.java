package com.indeed.imhotep.ez;

/**
 * @author vladimir
 */

public class ConstantDivideSingleStatReference extends SingleStatReference {
    private final long value;

    public ConstantDivideSingleStatReference(SingleStatReference stat, long value, EZImhotepSession session) {
        super(stat.depth, stat.toString() + "/" + value, session);
        if(value == 0) {
            throw new IllegalArgumentException("Can't divide by 0");
        }
        this.value = value;
    }

    @Override
    public double[] getGroupStats() {
        double[] results = super.getGroupStats();
        for(int i = 0; i < results.length; i++) {
            results[i] = results[i] / value;
        }
        return results;
    }

    @Override
    public double getValue(long[] stats) {
        return super.getValue(stats) / value;
    }
}
