package com.indeed.imhotep.ez;

/**
* @author jwolfe
*/
public class DynamicMetric {
    final String name;
    boolean valid = true;
    public DynamicMetric(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        if (valid) {
            return "dynamic:"+name;
        } else {
            return "invalid dynamic metric";
        }
    }
}
