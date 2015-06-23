package com.indeed.squall.jql.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

public class ExplodeMonthOfYear {
    public void execute(Session session) throws ImhotepOutOfMemoryException {
        new TimeRegroup(1, 'M', -360, Optional.<String>absent()).execute(session);
    }
}
