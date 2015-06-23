package com.indeed.squall.jql.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

import java.util.Optional;

public class ExplodeMonthOfYear {
    public void execute(Session session) throws ImhotepOutOfMemoryException {
        new TimeRegroup(1, 'M', -360, Optional.<String>empty()).execute(session);
    }
}
