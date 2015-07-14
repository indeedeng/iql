package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.Session;

import java.io.IOException;
import java.util.Collections;

public class GetNumGroups implements Command {
    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        out.accept(Session.MAPPER.writeValueAsString(Collections.singletonList(session.numGroups)));
    }
}
