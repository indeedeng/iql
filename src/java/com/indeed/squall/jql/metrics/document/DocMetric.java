package com.indeed.squall.jql.metrics.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

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
