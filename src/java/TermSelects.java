import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
* @author jwolfe
*/
public class TermSelects {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public boolean isIntTerm;
    public String stringTerm;
    public long intTerm;

    public final double[] selects;
    public double topMetric;

    TermSelects(boolean isIntTerm, String stringTerm, long intTerm, double[] selects, double topMetric) {
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
        this.selects = selects;
        this.topMetric = topMetric;
    }

    public JsonNode toJsonNode() {
        final ObjectNode node = objectMapper.createObjectNode();
        if (isIntTerm) {
            node.put("intTerm", intTerm);
        } else {
            node.put("stringTerm", stringTerm);
        }
        node.put("selects", objectMapper.valueToTree(selects));
        return node;
    }

    @Override
    public String toString() {
        return "TermSelects{" +
                "isIntTerm=" + isIntTerm +
                ", stringTerm='" + stringTerm + '\'' +
                ", intTerm=" + intTerm +
                ", selects=" + Arrays.toString(selects) +
                ", topMetric=" + topMetric +
                '}';
    }
}
