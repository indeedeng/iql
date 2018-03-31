package com.indeed.squall.iql2.execution.metrics.document;

import java.util.List;

/**
 * @author jwolfe
 */
public interface DocMetric {
    List<String> pushes();

    class BaseMetric implements DocMetric {
        private final List<String> push;

        public BaseMetric(List<String> push) {
            this.push = push;
        }

        @Override
        public List<String> pushes() {
            return push;
        }
    }
}
