package com.indeed.squall.iql2.execution.commands.misc;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

import java.io.IOException;

public interface IterateHandlerable<T> {
    IterateHandler<T> iterateHandler(Session session) throws ImhotepOutOfMemoryException, IOException;
}
