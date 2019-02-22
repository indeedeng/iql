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

package com.indeed.iql2.server.web.servlets.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.web.print.LevelPrinter;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class ExplainQueryExecution {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // IQL2 Imhotep-based state
    private final DatasetsMetadata datasetsMetadata;
    private final boolean isJSON;
    private final WallClock clock;
    private final Set<String> defaultIQL2Options;

    // Query output state
    private final PrintWriter outputStream;

    // Query inputs
    private final String query;
    private final int version;
    private final LevelPrinter printer;

    public ExplainQueryExecution(
            final DatasetsMetadata datasetsMetadata,
            final PrintWriter outputStream,
            final String query,
            final int version,
            final boolean isJSON,
            final WallClock clock,
            final IQL2Options defaultIQL2Options
    ) {
        this.datasetsMetadata = datasetsMetadata;
        this.outputStream = outputStream;
        this.query = query;
        this.version = version;
        this.isJSON = isJSON;
        this.clock = clock;
        this.defaultIQL2Options = defaultIQL2Options.getOptions();
        this.printer = new LevelPrinter();
    }

    public void processExplain() throws IOException {
        final Set<String> errors = new HashSet<>();
        final Set<String> warnings = new HashSet<>();
        final Consumer<String> warn = warnings::add;

        final TracingTreeTimer timer = new TracingTreeTimer();
        final Query parsedQuery = Queries.parseQuery(query, version==1, datasetsMetadata, defaultIQL2Options, warn, clock, timer, new NullShardResolver()).query;
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
        outputStream.close(); // only close on success because on error the stack trace is printed
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
            CommandValidator.validate(query, datasetsMetadata, new ErrorCollector(errors, warnings));

            for (final Command command : commands) {
                printer.push(command.toString());
                printer.pop();
            }
        }
    }
}
