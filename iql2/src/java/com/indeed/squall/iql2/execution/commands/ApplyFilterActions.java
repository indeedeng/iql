package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.actions.Action;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.util.List;

public class ApplyFilterActions implements Command {
    public final ImmutableList<Action> actions;

    public ApplyFilterActions(List<Action> actions) {
        this.actions = ImmutableList.copyOf(actions);
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        for (final Action action : actions) {
            session.timer.push("action.apply " + action);
            action.apply(session);
            session.timer.pop();
        }
        out.accept("Applied filters");
    }
}
