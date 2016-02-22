package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.indeed.common.util.time.DefaultWallClock;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.actions.Actions;
import com.indeed.squall.iql2.execution.commands.ApplyFilterActions;
import com.indeed.squall.iql2.execution.commands.IterateAndExplode;
import com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.Constant;
import com.indeed.squall.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.GroupSuppliers;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.actions.Action;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.util.core.TreeTimer;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
public class EventRegexServlet {
    private static final Logger log = Logger.getLogger(EventRegexServlet.class);

    private final ImhotepClient imhotepClient;
    private final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader;

    @Autowired
    public EventRegexServlet(
            final ImhotepClient imhotepClient,
            final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader
    ) {
        this.imhotepClient = imhotepClient;
        this.keywordAnalyzerWhitelistLoader = keywordAnalyzerWhitelistLoader;
    }

    @RequestMapping(value="regex", method = {RequestMethod.GET, RequestMethod.POST})
    @ResponseBody
    public String handle(
            @RequestParam("events") final String events,
            @RequestParam("startDate") final String startDate,
            @RequestParam("endDate") final String endDate,
            @RequestParam("regexp") final String regexp
    ) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        try (final Closer closer = Closer.create()) {
            // Man this Session API SUCKS. jwolfe sure messed that one up.
            final TreeTimer timer = new TreeTimer();
            final Consumer<String> out = new Consumer<String>() {
                @Override
                public void accept(String s) {
                    log.info(s);
                }
            };

            final JsonNode eventsArray = objectMapper.readTree(events);
            final List<EventDescription> eventDescriptions = new ArrayList<>();
            for (int i = 0; i < eventsArray.size(); i++) {
                final JsonNode entry = eventsArray.get(i);
                final Optional<DocFilter> filter;
                final String filterText = entry.get("filter").textValue();
                if (filterText.trim().isEmpty()) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(parseDocFilter(filterText));
                }
                final String characterText = entry.get("character").textValue();
                if (characterText.length() != 1) {
                    throw new IllegalArgumentException("character must be one character: [" + characterText + "]");
                }
                final char character = characterText.charAt(0);
                final String idField = entry.get("idField").textValue();
                final String index = entry.get("index").textValue();
                eventDescriptions.add(new EventDescription(character, index, filter, idField));
            }

            final List<Map<String, String>> datasets = new ArrayList<>();
            final Map<String, List<ShardIdWithVersion>> datasetToChosenShards = new HashMap<>();
            final DateTime start = DateTime.parse(startDate);
            final DateTime end = DateTime.parse(endDate);
            for (final EventDescription description : eventDescriptions) {
                datasets.add(ImmutableMap.of(
                        "dataset", description.index.toUpperCase(),
                        "start", start.toString(),
                        "end", end.toString(),
                        "fieldAliases", "{\"THEONETRUEJOINFIELD\": \"" + description.joinField.toUpperCase() + "\"}",
                        "name", description.name()
                ));
                datasetToChosenShards.put(description.name(), imhotepClient.sessionBuilder(description.index, start, end).getChosenShards());
            }

            final String json = objectMapper.writeValueAsString(datasets);
            final Session.CreateSessionResult createSessionResult = Session.createSession(
                    imhotepClient,
                    datasetToChosenShards,
                    objectMapper.readTree(json),
                    closer,
                    out,
                    Collections.<String, DatasetDimensions>emptyMap(),
                    timer,
                    new NoOpProgressCallback(),
                    -1L,
                    -1L,
                    new DefaultWallClock(),
                    "jwolfe"
            );
            final Session session = createSessionResult.session.get();

            for (final EventDescription description : eventDescriptions) {
                if (description.filter.isPresent()) {
                    final DocFilter docFilter = description.filter.get();
                    final List<Action> actions = docFilter.getExecutionActions(ImmutableMap.of(description.name(), description.index.toUpperCase()), 1, 1, 0, GroupSuppliers.newGroupSupplier(2));
                    // WHAT HAVE I DONE?
                    final List<com.indeed.squall.iql2.execution.actions.Action> executionActions = new ArrayList<>();
                    for (final Action action : actions) {
                        // SERIOUSLY?
                        executionActions.add(Actions.parseFrom(objectMapper.readTree(objectMapper.writeValueAsString(action))));
                    }
                    new ApplyFilterActions(executionActions).execute(session, out);
                }
            }

            final Automaton automaton = new RegExp(regexp).toAutomaton(true);
            final Set<String> required = new HashSet<>();
            for (final EventDescription description : eventDescriptions) {
                final char c = description.c;
                final Automaton noCs = new RegExp("[^" + c + "]*").toAutomaton(false);
                if (automaton.intersection(noCs).isEmpty()) {
                    required.add(description.name());
                    log.info(c + " is required");
                }
            }

            final RunAutomaton runAutomaton = new RunAutomaton(automaton);

            final FieldIterateOpts fieldOpts = new FieldIterateOpts();
            if (required.size() > 0) {
                AggregateFilter filter = null;
                for (final String dataset : required) {
                    final AggregateFilter.GreaterThan singleFilter = new AggregateFilter.GreaterThan(new DocumentLevelMetric(dataset, Collections.singletonList("count()")), new Constant(0));
                    if (filter == null) {
                        filter = singleFilter;
                    } else {
                        filter = new AggregateFilter.And(filter, singleFilter);
                    }
                }
                fieldOpts.filter = Optional.of(filter);
            }
            new IterateAndExplode("THEONETRUEJOINFIELD", Collections.<AggregateMetric>emptyList(), fieldOpts, Optional.<String>absent(), required.isEmpty() ? null : required).execute(session, out);
            final GroupKeySet groupKeySet = session.groupKeySet;


            final int[] states = new int[groupKeySet.numGroups() + 1];
            Arrays.fill(states, runAutomaton.getInitialState());

            final Set<QualifiedPush> qualifiedPushes = new HashSet<>();
            final Map<QualifiedPush, Character> pushToChar = new HashMap<>();
            for (final EventDescription description : eventDescriptions) {
                final QualifiedPush push = new QualifiedPush(description.name(), Collections.singletonList("count()"));
                qualifiedPushes.add(push);
                pushToChar.put(push, description.c);
            }
            final Map<QualifiedPush, Integer> metricIndexes = new HashMap<>();
            final Map<String, IntList> sessionMetricIndexes = new HashMap<>();
            session.pushMetrics(qualifiedPushes, metricIndexes, sessionMetricIndexes);

            final char[] indexToChar = new char[eventDescriptions.size()];
            for (final QualifiedPush push : qualifiedPushes) {
                final int index = metricIndexes.get(push);
                final char c = pushToChar.get(push);
                indexToChar[index] = c;
            }

            final IterateCallback callback = new IterateCallback(indexToChar, states, runAutomaton);
            Session.iterateMultiInt(session.getSessionsMapRaw(), sessionMetricIndexes, "UNIXTIME", callback);
            final BitSet matched = new BitSet(states.length);
            for (int i = 0; i < states.length; i++) {
                if (runAutomaton.isAccept(states[i])) {
                    matched.set(i);
                }
            }
            final int numMatched = matched.cardinality();
            final int collisionCount = callback.collisionCount;
            final List<String> ctks = new ArrayList<>();
            for (int i = matched.nextSetBit(0); i != -1; i = matched.nextSetBit(i + 1)) {
                final GroupKey groupKey = session.groupKeySet.groupKey(i);
                if (groupKey instanceof StringGroupKey) {
                    ctks.add(((StringGroupKey) groupKey).term);
                } else if (groupKey instanceof IntTermGroupKey) {
                    ctks.add(String.valueOf(((IntTermGroupKey) groupKey).value));
                }
            }
            return String.valueOf(numMatched) + "\t" + collisionCount + "\n" + ctks;
        }
    }

    private static class IterateCallback implements Session.IntIterateCallback {
        private int collisionCount = 0;

        private final char[] indexToChar;
        private final int[] states;
        private final RunAutomaton runAutomaton;

        private IterateCallback(char[] indexToChar, int[] states, RunAutomaton runAutomaton) {
            this.indexToChar = indexToChar;
            this.states = states;
            this.runAutomaton = runAutomaton;
        }

        @Override
        public void term(long term, long[] stats, int group) {
            if (states[group] == -1) {
                return;
            }
            boolean found = false;
            for (int i = 0; i < stats.length; i++) {
                if (stats[i] > 0) {
                    if (found) {
                        collisionCount++;
                        continue;
                    }
                    found = true;
                    final char c = indexToChar[i];
                    states[group] = runAutomaton.step(states[group], c);
                }
            }
        }
    }


    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        // TODO: Don't make a copy per use
        return QueryServlet.upperCaseMapToSet(keywordAnalyzerWhitelistLoader.getKeywordAnalyzerWhitelist());
    }

    private Map<String, Set<String>> getDatasetToIntFields() throws IOException {
        // TODO: Don't make a copy per use
        return QueryServlet.upperCaseMapToSet(keywordAnalyzerWhitelistLoader.getDatasetToIntFields());
    }

    private DocFilter parseDocFilter(String filterString) throws IOException {
        JQLParser.JqlDocFilterContext docFilterContext = Queries.runParser(filterString, new Function<JQLParser, JQLParser.JqlDocFilterContext>() {
            public JQLParser.JqlDocFilterContext apply(JQLParser input) {
                return input.jqlDocFilter();
            }
        });
        final com.indeed.squall.iql2.language.compat.Consumer<String> noOpWarn = new com.indeed.squall.iql2.language.compat.Consumer<String>() {
            @Override
            public void accept(String s) {
            }
        };
        return DocFilters.parseJQLDocFilter(docFilterContext, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), null, noOpWarn, new DefaultWallClock());
    }

    public static class EventDescription {
        public final char c;
        public final String index;
        public final Optional<DocFilter> filter;
        public final String joinField;

        public EventDescription(char c, String index, Optional<DocFilter> filter, String joinField) {
            this.c = c;
            this.index = index;
            this.filter = filter;
            this.joinField = joinField;
        }

        public String name() {
            return String.valueOf(c).toUpperCase();
        }
    }
}
