package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

public class ExplodeMonthOfYear implements Command {
    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        new TimeRegroup(1, 'M', -360, Optional.<String>absent()).execute(session, out);
    }
}
