package com.indeed.squall.iql2.language.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Actions {
    public static boolean groupsAndScopeMatch(QueryAction q1, QueryAction q2) {
        return scopeMatches(q1, q2) && targetMatches(q1, q2) && q1.negativeGroup == q2.negativeGroup && q1.positiveGroup == q2.positiveGroup;
    }

    private static boolean targetMatches(QueryAction q1, QueryAction q2) {
        return q1.targetGroup == q2.targetGroup;
    }

    private static boolean scopeMatches(QueryAction q1, QueryAction q2) {
        return q1.scope.equals(q2.scope);
    }

    private static boolean resultGroupsOppose(QueryAction q1, QueryAction q2) {
        return q1.negativeGroup == q2.positiveGroup && q1.positiveGroup == q2.negativeGroup;
    }

    public static List<Action> optimizeConsecutiveQueryActions(List<Action> actions) {
        final List<Action> result = new ArrayList<>();
        QueryAction currentQueryAction = null;
        for (final Action action : actions) {
            if (action instanceof QueryAction) {
                final QueryAction queryAction = (QueryAction) action;
                if (currentQueryAction == null) {
                    currentQueryAction = queryAction;
                } else if (groupsAndScopeMatch(currentQueryAction, queryAction)) {
                    if (queryAction.targetGroup == queryAction.negativeGroup) {
                        final Map<String, Query> perDatasetQuery = new HashMap<>();
                        for (final String dataset : queryAction.scope) {
                            perDatasetQuery.put(dataset, Query.newBooleanQuery(BooleanOp.OR, Arrays.asList(currentQueryAction.perDatasetQuery.get(dataset), queryAction.perDatasetQuery.get(dataset))));
                        }
                        currentQueryAction = new QueryAction(queryAction.scope, perDatasetQuery, queryAction.targetGroup, queryAction.positiveGroup, queryAction.negativeGroup);
                    } else if (queryAction.targetGroup == queryAction.positiveGroup) {
                        final Map<String, Query> perDatasetQuery = new HashMap<>();
                        for (final String dataset : queryAction.scope) {
                            perDatasetQuery.put(dataset, Query.newBooleanQuery(BooleanOp.AND, Arrays.asList(currentQueryAction.perDatasetQuery.get(dataset), queryAction.perDatasetQuery.get(dataset))));
                        }
                        currentQueryAction = new QueryAction(queryAction.scope, perDatasetQuery, queryAction.targetGroup, queryAction.positiveGroup, queryAction.negativeGroup);
                    } else {
                        result.add(currentQueryAction);
                        currentQueryAction = queryAction;
                    }
//                } else if (scopeMatches(currentQueryAction, queryAction)
//                            && targetMatches(currentQueryAction, queryAction)
//                            && resultGroupsOppose(currentQueryAction, queryAction)) {
                    // TODO: Make this optimization not suck, and re-enable.
//                    final Map<String, Query> perDatasetQuery = new HashMap<>();
//                    for (final String dataset : queryAction.scope) {
//                        perDatasetQuery.put(
//                                dataset,
//                                Query.newBooleanQuery(
//                                    BooleanOp.NOT,
//                                    Collections.singletonList(
//                                        Query.newBooleanQuery(
//                                            BooleanOp.OR,
//                                            Arrays.asList(
//                                                    Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(currentQueryAction.perDatasetQuery.get(dataset))),
//                                                    Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(queryAction.perDatasetQuery.get(dataset)))
//                                            )
//                                        )
//                                    )
//                                )
//                        );
//                    }
//                    currentQueryAction = new QueryAction(currentQueryAction.scope, perDatasetQuery, currentQueryAction.targetGroup, currentQueryAction.positiveGroup, currentQueryAction.negativeGroup);
                } else {
                    result.add(currentQueryAction);
                    currentQueryAction = queryAction;
                }
            } else {
                if (currentQueryAction != null) {
                    result.add(currentQueryAction);
                    currentQueryAction = null;
                }
                result.add(action);
            }
        }
        if (currentQueryAction != null) {
            result.add(currentQueryAction);
        }
        return result;
    }

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static JsonNode queryJson(Query query) {
        final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.set("type", JsonNodeFactory.instance.textNode(query.getQueryType().name()));
        switch (query.getQueryType()) {
            case TERM:
                objectNode.set("startTerm", termJson(query.getStartTerm()));
                break;
            case BOOLEAN:
                objectNode.set("operator", JsonNodeFactory.instance.textNode(query.getOperator().name()));
                final ArrayNode operands = OBJECT_MAPPER.createArrayNode();
                for (final Query operand : query.getOperands()) {
                    operands.add(queryJson(operand));
                }
                objectNode.set("operands", operands);
                break;
            case RANGE:
                objectNode.set("startTerm", termJson(query.getStartTerm()));
                objectNode.set("endTerm", termJson(query.getEndTerm()));
                objectNode.set("isMaxInclusive", JsonNodeFactory.instance.booleanNode(query.isMaxInclusive()));
                break;
        }
        return objectNode;
    }

    private static @Nullable JsonNode termJson(@Nullable Term term) {
        if (term == null) {
            return null;
        }
        final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.set("field", JsonNodeFactory.instance.textNode(term.getFieldName()));
        objectNode.set("isIntField", JsonNodeFactory.instance.booleanNode(term.isIntField()));
        if (term.isIntField()) {
            objectNode.set("intTerm", JsonNodeFactory.instance.numberNode(term.getTermIntVal()));
        } else {
            objectNode.set("stringTerm", JsonNodeFactory.instance.textNode(term.getTermStringVal()));
        }
        return objectNode;
    }
}
