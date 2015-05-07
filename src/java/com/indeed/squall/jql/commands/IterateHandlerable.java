package com.indeed.squall.jql.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

import java.io.IOException;

public interface IterateHandlerable<T> {
    IterateHandler<T> iterateHandler(Session session) throws ImhotepOutOfMemoryException, IOException;
}
