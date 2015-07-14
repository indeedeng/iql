package com.indeed.squall.jql.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;

public class ExplodeMonthOfYear implements Command {
    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        new TimeRegroup(1, 'M', -360, Optional.<String>absent()).execute(session, out);
    }
}
