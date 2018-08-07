/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;;

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
