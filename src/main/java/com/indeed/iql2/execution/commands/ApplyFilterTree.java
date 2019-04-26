/*
 * Copyright (C) 2019 Indeed Inc.
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
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.language.GroupNameSupplier;
import com.indeed.iql2.language.passes.BooleanFilterTree;

import java.util.Map;

public class ApplyFilterTree implements Command {
    public final BooleanFilterTree tree;

    public ApplyFilterTree(BooleanFilterTree tree) {
        this.tree = tree;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            final String dataset = entry.getKey();
            final ImhotepSession imhotepSession = entry.getValue().session.getSession();
            final BooleanFilterTree tree = this.tree.applyQualifieds(dataset);
            if (tree instanceof BooleanFilterTree.Constant) {
                if (((BooleanFilterTree.Constant) tree).isValue()) {
                    // do nothing
                } else {
                    // throw everything away :(
                    imhotepSession.regroup(new int[]{1}, new int[]{0}, true);
                }
            } else {
                final String outputGroupsName = tree.apply(dataset, imhotepSession, new GroupNameSupplier());
                imhotepSession.regroup(new RegroupParams(outputGroupsName, ImhotepSession.DEFAULT_GROUPS), new int[]{1}, new int[]{1}, true);
            }
        }
    }
}
