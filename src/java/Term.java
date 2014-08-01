import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

/**
 * @author jwolfe
 */
public class Term {
    private static final Logger log = Logger.getLogger(Term.class);

    public final boolean isIntTerm;
    public final String stringTerm;
    public final long intTerm;

    public Term(boolean isIntTerm, String stringTerm, long intTerm) {
        this.isIntTerm = isIntTerm;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
    }

    public static Term fromJson(JsonNode node) {
        switch (node.get("type").asText()) {
            case "string":
                return new Term(false, node.get("value").asText(), 0);
            case "int":
                return new Term(true, null, node.get("value").asLong());
        }
        throw new RuntimeException("Oops: " + node);
    }
}
