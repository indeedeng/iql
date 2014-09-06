import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface DocMetric {
    public List<String> pushes();

    public static DocMetric fromJson(JsonNode node) {
        switch (node.get("type").asText()) {
            case "docStats": {
                final JsonNode pushes = node.get("pushes");
                final List<String> statPushes = Lists.newArrayList();
                for (final JsonNode push : pushes) {
                    statPushes.add(push.asText());
                }
                return new BaseMetric(statPushes);
            }
        }
        throw new RuntimeException("Oops: " + node);
    }

    static class BaseMetric implements DocMetric {
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
