package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.io.IOException;

/**
 * Corner case of distinct calculation for one dataset without filter
 */
public class GetSimpleGroupDistincts implements Command {
    public final String scope;
    public final String field;

    public GetSimpleGroupDistincts(final String scope, final String field) {
        this.scope = scope;
        this.field = field;
    }

    @Override
    public void execute(final Session session, final Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final long[] groupCounts = session.getSimpleDistinct(field, scope);
        out.accept(Session.MAPPER.writeValueAsString(groupCounts));
    }

    @Override
    public String toString() {
        return "GetSimpleGroupDistincts{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                '}';
    }
}
