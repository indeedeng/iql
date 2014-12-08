/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.ez;

/**
 * @author vladimir
 */

public class CompositeStatReference implements StatReference {
    StatReference ref1;
    StatReference ref2;

    public CompositeStatReference(StatReference ref1, StatReference ref2) {
        this.ref1 = ref1;
        this.ref2 = ref2;
    }

    @Override
    public boolean isValid() {
        return ref1.isValid() && ref2.isValid();
    }

    @Override
    public void invalidate() {
        ref1.invalidate();
        ref2.invalidate();
    }

    @Override
    public double getValue(long[] stats) {
        return applyComposite(ref1.getValue(stats), ref2.getValue(stats));
    }

    private static double applyComposite(double val1, double val2) {
        return val1 / val2;
    }

    @Override
    public double[] getGroupStats() {
        double[] stats1 = ref1.getGroupStats();
        double[] stats2 = ref2.getGroupStats();

        int resultLength = Math.max(stats1.length, stats2.length);
        double[] finalStats = new double[resultLength];
        for(int i = 0; i < resultLength; i++) {
            double val1 = i < stats1.length ? stats1[i] : 0;
            double val2 = i < stats2.length ? stats2[i] : 0;
            finalStats[i] = applyComposite(val1, val2);
        }
        return finalStats;
    }

    @Override
    public String toString() {
        if (isValid()) {
            return ref1.toString(); // ref1 and ref2 toString() end up equal and contain both stats
        } else {
            return "invalid stat reference";
        }
    }

    public StatReference getRef1() {
        return ref1;
    }

    public StatReference getRef2() {
        return ref2;
    }
}
