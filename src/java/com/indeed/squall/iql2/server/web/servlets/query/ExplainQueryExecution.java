package com.indeed.squall.iql2.server.web.servlets.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.indeed.common.util.time.WallClock;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.server.print.LevelPrinter;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class ExplainQueryExecution {
    private static final Logger log = Logger.getLogger(ExplainQueryExecution.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // IQL2 Imhotep-based state
    private final ImhotepClient imhotepClient;
    private final Map<String, Set<String>> keywordAnalyzerWhitelist;
    private final Map<String, Set<String>> datasetToIntFields;
    private final Map<String, DatasetDimensions> dimensions;
    private final boolean isJSON;
    private final WallClock clock;

    // Query output state
    private final PrintWriter outputStream;

    // Query inputs
    private final String query;
    private final int version;
    private final LevelPrinter printer;

    private boolean ran = false;

    public ExplainQueryExecution(
            final ImhotepClient imhotepClient,
            final Map<String, Set<String>> keywordAnalyzerWhitelist,
            final Map<String, Set<String>> datasetToIntFields,
            final Map<String, DatasetDimensions> dimensions,
            final PrintWriter outputStream,
            final String query,
            final int version,
            final boolean isJSON,
            final WallClock clock
    ) {
        this.outputStream = outputStream;
        this.query = query;
        this.version = version;
        this.keywordAnalyzerWhitelist = keywordAnalyzerWhitelist;
        this.datasetToIntFields = datasetToIntFields;
        this.imhotepClient = imhotepClient;
        this.dimensions = dimensions;
        this.isJSON = isJSON;
        this.clock = clock;
        this.printer = new LevelPrinter();
    }

    public void processExplain() throws TimeoutException, IOException, ImhotepOutOfMemoryException {
        final Set<String> errors = new HashSet<>();
        final Set<String> warnings = new HashSet<>();
        final Consumer<String> warn = new Consumer<String>() {
            @Override
            public void accept(String s) {
                warnings.add(s);
            }
        };

        final Query parsedQuery = Queries.parseQuery(query, version==1, keywordAnalyzerWhitelist, datasetToIntFields, warn, clock);
        new ParsedQueryExplain(parsedQuery, errors, warnings).explainParsedQuery();
        if (!isJSON) {
            outputStream.println(printer.toString());
            if (!errors.isEmpty()) {
                outputStream.printf("\n\nErrors:\n");
                outputStream.println(Joiner.on('\n').join(errors));
            }
            if (!warnings.isEmpty()) {
                outputStream.printf("\n\nWarnings:\n");
                outputStream.println(Joiner.on('\n').join(warnings));
            }
        } else {
            final Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("explain", printer.toString());
            dataMap.put("errors", errors);
            dataMap.put("warnings", warnings);
            outputStream.println(OBJECT_MAPPER.writeValueAsString(dataMap));
        }
    }

    private class ParsedQueryExplain {
        private final Set<String> warnings;
        private final Set<String> errors;
        private final Query originalQuery;

        private ParsedQueryExplain(Query query, final Set<String> errors, final Set<String> warnings) {
            originalQuery = query;
            this.errors = errors;
            this.warnings = warnings;
        }

        private void explainParsedQuery() {
            final Query query = originalQuery.transform(
                    Functions.<GroupBy>identity(),
                    Functions.<AggregateMetric>identity(),
                    Functions.<DocMetric>identity(),
                    Functions.<AggregateFilter>identity(),
                    new Function<DocFilter, DocFilter>() {
                        @Nullable
                        @Override
                        public DocFilter apply(DocFilter input) {
                            if (input instanceof DocFilter.FieldInQuery) {
                                final DocFilter.FieldInQuery fieldInQuery = (DocFilter.FieldInQuery) input;
                                final Query q = fieldInQuery.query;
                                printer.push("sub-query: \"" + q + "\"");
                                new ExplainQueryExecution.ParsedQueryExplain(q, errors, warnings).explainParsedQuery();
                                printer.pop();
                                return new DocFilter.ExplainFieldIn(q, fieldInQuery.field, fieldInQuery.isNegated);
                            }
                            return input;
                        }
                    }
            );

            final List<Command> commands = Queries.queryCommands(query);
            CommandValidator.validate(commands, imhotepClient, query, dimensions, datasetToIntFields, errors, warnings);

            for (final Command command : commands) {
                printer.push(command.toString());
                printer.pop();
            }
        }
    }
}
