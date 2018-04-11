/*
 * Copyright (C) 2018 Indeed Inc.
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
 package com.indeed.imhotep.web;

import com.indeed.imhotep.sql.parser.TerminalParser;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
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
