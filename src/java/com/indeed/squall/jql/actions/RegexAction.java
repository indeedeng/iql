package com.indeed.squall.jql.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

import java.util.Map;
import java.util.Set;

public class RegexAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final String regex;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public RegexAction(Set<String> scope, String field, String regex, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.regex = regex;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("RegexAction");
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                final Session.ImhotepSessionInfo v = entry.getValue();
                v.session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
            }
        }
        session.timer.pop();
    }
}
