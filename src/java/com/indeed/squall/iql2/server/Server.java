package com.indeed.squall.iql2.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;
import com.indeed.squall.jql.dimensions.DatasetDimensions;
import com.indeed.util.core.TreeTimer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.ServletRequestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
public class Server {
    private static final Logger log = Logger.getLogger(Server.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ImhotepClient imhotepClient;

    @Autowired
    public Server(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        return Collections.emptyMap();
    }

    private Map<String, Set<String>> getDatasetToIntFields() {
        return Collections.emptyMap();
    }

    @RequestMapping("/query")
    public void handle(final HttpServletRequest request,
                       final HttpServletResponse response,
                       final @Nonnull @RequestParam("q") String query
    ) throws ServletException, IOException, ImhotepOutOfMemoryException {
        final int version = ServletRequestUtils.getIntParameter(request, "v", 1);
        final ServletOutputStream outputStream = response.getOutputStream();
        executeQuery(query, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), new Consumer<String>() {
            @Override
            public void accept(String s) {
                try {
                    outputStream.println(s);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    public void executeQuery(String q, boolean useLegacy, Map<String, Set<String>> keywordAnalyzerWhitelist, Map<String, Set<String>> datasetToIntFields, Consumer<String> out) throws IOException, ImhotepOutOfMemoryException {
        final Query query = Queries.parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields);
        final List<Command> commands = Queries.queryCommands(query);
        final ObjectMapper objectMapper = new ObjectMapper();
        if (log.isTraceEnabled()) {
            log.trace("commands = " + commands);
            for (final Command command : commands) {
                log.trace("command = " + command);
                final String s = objectMapper.writeValueAsString(command);
                log.trace("s = " + s);
            }
            final String commandList = objectMapper.writeValueAsString(commands);
            log.trace("commandList = " + commandList);
        }

        final Map<String, Object> request = new HashMap<>();
        request.put("datasets", Queries.createDatasetMap(query));
        request.put("commands", commands);

        final JsonNode requestJson = OBJECT_MAPPER.valueToTree(request);

        try (final Closer closer = Closer.create()) {
            final TreeTimer timer = new TreeTimer();
            timer.push(q);
            Session.createSession(imhotepClient, requestJson, closer, out, Collections.<String, DatasetDimensions>emptyMap(), timer);
            timer.pop();
            System.out.println(timer);
        }
    }
}
