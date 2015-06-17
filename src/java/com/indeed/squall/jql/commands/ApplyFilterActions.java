package com.indeed.squall.jql.commands;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.actions.Action;

import java.util.List;

public class ApplyFilterActions {
    public final ImmutableList<Action> actions;

    public ApplyFilterActions(List<Action> actions) {
        this.actions = ImmutableList.copyOf(actions);
    }

    public void execute(Session session) throws ImhotepOutOfMemoryException {
        for (final Action action : actions) {
            action.apply(session);
        }
    }
}
