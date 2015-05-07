package com.indeed.squall.jql.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Pushable;
import com.indeed.squall.jql.Session;

import java.io.IOException;
import java.util.Set;

public interface IterateHandler<T> extends Pushable {
    Set<String> scope();
    Session.IntIterateCallback intIterateCallback();
    Session.StringIterateCallback stringIterateCallback();
    T finish() throws ImhotepOutOfMemoryException, IOException;
}
