package com.indeed.imhotep.web;

import com.indeed.imhotep.sql.parser.TerminalParser;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jparsec.Token;
import org.codehaus.jparsec.Tokens;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * @author vladimir
 */
@Controller
public class TokenizeController {
    private static final Logger log = Logger.getLogger(TokenizeController.class);

    @RequestMapping("/tokenize")
    @ResponseBody
    protected void handleTokenize(@RequestParam("q") String query,
                                      HttpServletResponse resp) throws IOException {

        resp.setHeader("Access-Control-Allow-Origin", "*");
        try {
            final List<Token> tokens = TerminalParser.LEXER.parse(query);
            final ServletOutputStream outputStream = resp.getOutputStream();
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
            final ObjectNode jsonRoot = mapper.createObjectNode();
            final ArrayNode tokenArrayNode= jsonRoot.arrayNode();
            jsonRoot.put("tokens", tokenArrayNode);
            for(Token token : tokens) {
                final ObjectNode tokenNode = tokenArrayNode.objectNode();
                final String text = token.value() instanceof Tokens.Fragment ? ((Tokens.Fragment)token.value()).text() : (String) token.value();
                final String tag = token.value() instanceof Tokens.Fragment ? ((Tokens.Fragment)token.value()).tag().toString() : "STRING";

                tokenNode.put("text", text);
                tokenNode.put("index", token.index());
                tokenNode.put("length", token.length());
                tokenNode.put("tag", tag);
                tokenArrayNode.add(tokenNode);
            }

            mapper.writeValue(outputStream, jsonRoot);
            outputStream.close();

        } catch (Throwable e) {
            QueryServlet.handleError(resp, true, e, false, false);
        }
    }
}
