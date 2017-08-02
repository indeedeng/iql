package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.io.IOException;

public interface Command {
    void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException;
}
