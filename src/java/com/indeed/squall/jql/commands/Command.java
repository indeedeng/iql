package com.indeed.squall.jql.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;

import java.io.IOException;

public interface Command {
    void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException;
}
