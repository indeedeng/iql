package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.common.util.time.DefaultWallClock;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.actions.Actions;
import com.indeed.squall.iql2.execution.commands.ApplyFilterActions;
import com.indeed.squall.iql2.execution.commands.TimePeriodRegroup;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.GroupSuppliers;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.actions.Action;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.squall.iql2.server.web.servlets.query.QueryServlet;
import com.indeed.util.core.Pair;
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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
public class EventRegexServlet {
    private static final Logger log = Logger.getLogger(EventRegexServlet.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String JOIN_FIELD = "THEONETRUEJOINFIELD";

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
        log.info("Request received");
        try (final Closer closer = Closer.create()) {
            // Man this Session API SUCKS. jwolfe sure messed that one up.
            final TreeTimer timer = new TreeTimer();

            timer.push("total");

            final Consumer<String> out = new Consumer<String>() {
                @Override
                public void accept(String s) {
                    log.info(s);
                }
            };

            final List<EventDescription> eventDescriptions = getEventDescriptions(OBJECT_MAPPER.readTree(events));

            final CreateSessionResult createSessionResult = createSession(startDate, endDate, closer, timer, out, eventDescriptions);
            final Session session = createSessionResult.session;

            for (final EventDescription description : eventDescriptions) {
                if (description.filter.isPresent()) {
                    final DocFilter docFilter = description.filter.get();
                    final List<Action> actions = docFilter.getExecutionActions(ImmutableMap.of(description.name(), description.index.toUpperCase()), 1, 1, 0, GroupSuppliers.newGroupSupplier(2));
                    // WHAT HAVE I DONE?
                    final List<com.indeed.squall.iql2.execution.actions.Action> executionActions = new ArrayList<>();
                    for (final Action action : actions) {
                        // SERIOUSLY?
                        executionActions.add(Actions.parseFrom(OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(action))));
                    }
                    new ApplyFilterActions(executionActions).execute(session, out);
                }
            }

            final Automaton automaton = new RegExp(regexp).toAutomaton();

            new TimePeriodRegroup(1000, Optional.<String>absent(), Optional.<String>absent(), false).execute(session, out);

            final Map<String, Integer> presenceIndexes = new HashMap<>();
            final Map<Integer, Character> indexToChar = new HashMap<>();

            for (final EventDescription eventDescription : eventDescriptions) {
                final int ix = presenceIndexes.size();
                presenceIndexes.put(eventDescription.name(), ix);
                indexToChar.put(ix, eventDescription.c);
            }

            final StringBuilder output = new StringBuilder();
            final Map<String, Integer> sequenceToCounts;
            final Map<String, ?> representative;

            if (session.isIntField(JOIN_FIELD)) {
                final IntCallback callback = new IntCallback(automaton, indexToChar);
                Session.iterateMultiInt(session.getSessionsMapRaw(), Collections.<String, IntList>emptyMap(), presenceIndexes, JOIN_FIELD, callback);
                sequenceToCounts = callback.sequenceToCounts;
                representative = callback.representative;
            } else if (session.isStringField(JOIN_FIELD)) {
                final StringCallback callback = new StringCallback(automaton, indexToChar);
                Session.iterateMultiString(session.getSessionsMapRaw(), Collections.<String, IntList>emptyMap(), presenceIndexes, JOIN_FIELD, callback);
                sequenceToCounts = callback.sequenceToCounts;
                representative = callback.representative;
            } else {
                throw new IllegalStateException("Unknown field or something: " + JOIN_FIELD);
            }

            final BoundedPriorityQueue<Pair<Integer, String>> pq = BoundedPriorityQueue.newInstance(100, new Pair.HalfPairComparator());
            for (final Map.Entry<String, Integer> entry : sequenceToCounts.entrySet()) {
                pq.offer(Pair.of(entry.getValue(), entry.getKey()));
            }
            while (!pq.isEmpty()) {
                final Pair<Integer, String> p = pq.poll();
                output.append(String.format("%s: %d\t%s\n", p.getSecond(), p.getFirst(), representative.get(p.getSecond())));
            }

            timer.pop();

            log.info(timer.toString());

            return output.toString();
        }
    }

    private static class IntCallback implements Session.IntIterateCallback {
        private final Automaton automaton;
        private final Map<Integer, Character> indexToChar;

        private final Map<String, Integer> sequenceToCounts = new HashMap<>();
        private final Map<String, Long> representative = new HashMap<>();

        private boolean anySeen = false;
        private long lastTerm = Long.MAX_VALUE;

        private final StringBuilder sb = new StringBuilder();
        private final StringBuilder regexSb = new StringBuilder();

        private IntCallback(Automaton automaton, Map<Integer, Character> indexToChar) {
            this.automaton = automaton;
            this.indexToChar = indexToChar;
        }

        @Override
        public void term(long term, long[] stats, int group) {
            if (anySeen && term != lastTerm) {
                final String sequence = sb.toString();
                final boolean matches = !automaton.intersection(new RegExp(this.regexSb.toString()).toAutomaton()).isEmpty();
                if (matches) {
                    log.info(String.format("%s: %s", term, sequence));
                    Integer curCount = sequenceToCounts.get(sequence);
                    if (curCount == null) {
                        curCount = 0;
                        representative.put(sequence, lastTerm);
                    }
                    sequenceToCounts.put(sequence, curCount + 1);
                }
                sb.setLength(0);
                regexSb.setLength(0);
            }
            anySeen = true;
            lastTerm = term;
            boolean anyFound = false;
            regexSb.append('(');
            for (int i = 0; i < stats.length; i++) {
                if (stats[i] == 1) {
                    final char c = indexToChar.get(i);
                    if (anyFound) {
                        regexSb.append('|');
                    }
                    regexSb.append(c);
                    if (!anyFound) {
                        sb.append(c);
                    }
                    anyFound = true;
                }
            }
            regexSb.append(')');
        }
    }

    private static class StringCallback implements Session.StringIterateCallback {
        private final Automaton automaton;
        private final Map<Integer, Character> indexToChar;

        private final Map<String, Integer> sequenceToCounts = new HashMap<>();
        private final Map<String, String> representative = new HashMap<>();

        private boolean anySeen = false;
        private String lastTerm = null;

        private final StringBuilder sb = new StringBuilder();
        private final StringBuilder regexSb = new StringBuilder();

        private StringCallback(Automaton automaton, Map<Integer, Character> indexToChar) {
            this.automaton = automaton;
            this.indexToChar = indexToChar;
        }

        @Override
        public void term(String term, long[] stats, int group) {
            if (anySeen && term.equals(lastTerm)) {
                final String sequence = sb.toString();
                final boolean matches = !automaton.intersection(new RegExp(this.regexSb.toString()).toAutomaton()).isEmpty();
                if (matches) {
                    log.info(String.format("%s: %s", term, sequence));
                    Integer curCount = sequenceToCounts.get(sequence);
                    if (curCount == null) {
                        curCount = 0;
                        representative.put(sequence, lastTerm);
                    }
                    sequenceToCounts.put(sequence, curCount + 1);
                }
                sb.setLength(0);
                regexSb.setLength(0);
            }
            anySeen = true;
            lastTerm = term;
            boolean anyFound = false;
            regexSb.append('(');
            for (int i = 0; i < stats.length; i++) {
                if (stats[i] == 1) {
                    final char c = indexToChar.get(i);
                    if (anyFound) {
                        regexSb.append('|');
                    }
                    regexSb.append(c);
                    if (!anyFound) {
                        sb.append(c);
                    }
                    anyFound = true;
                }
            }
            regexSb.append(')');
        }
    }

    private static class CreateSessionResult {
        public final Session session;
        public final int numShards;

        private CreateSessionResult(Session session, int numShards) {
            this.session = session;
            this.numShards = numShards;
        }
    }

    private CreateSessionResult createSession(@RequestParam("startDate") String startDate, @RequestParam("endDate") String endDate, Closer closer, TreeTimer timer, Consumer<String> out, List<EventDescription> eventDescriptions) throws com.indeed.imhotep.api.ImhotepOutOfMemoryException, IOException {
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

        final String json = OBJECT_MAPPER.writeValueAsString(datasets);
        final Session.CreateSessionResult createSessionResult = Session.createSession(
                imhotepClient,
                datasetToChosenShards,
                OBJECT_MAPPER.readTree(json),
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

        int numShards = 0;
        for (final List<ShardIdWithVersion> shards : datasetToChosenShards.values()) {
            numShards += shards.size();
        }

        return new CreateSessionResult(createSessionResult.session.get(), numShards);
    }

    @Nonnull
    private List<EventDescription> getEventDescriptions(JsonNode eventsArray) throws IOException {
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
        return eventDescriptions;
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
