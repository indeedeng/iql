import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author jwolfe
 */
public interface DocMetric {
    List<String> pushes();

    static DocMetric fromJson(JsonNode node) {
        switch (node.get("type").textValue()) {
            case "docStats": {
                final JsonNode pushes = node.get("pushes");
                final List<String> statPushes = Lists.newArrayList();
                for (final JsonNode push : pushes) {
                    statPushes.add(push.textValue());
                }
                return new BaseMetric(statPushes);
            }
        }
        throw new RuntimeException("Oops: " + node);
    }

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
