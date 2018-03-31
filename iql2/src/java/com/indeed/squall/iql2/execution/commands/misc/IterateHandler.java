package com.indeed.squall.iql2.execution.commands.misc;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Pushable;
import com.indeed.squall.iql2.execution.Session;

import java.io.IOException;
import java.util.Set;

public interface IterateHandler<T> extends Pushable {
    Set<String> scope();
    Session.IntIterateCallback intIterateCallback();
    Session.StringIterateCallback stringIterateCallback();
    T finish() throws ImhotepOutOfMemoryException, IOException;
}
