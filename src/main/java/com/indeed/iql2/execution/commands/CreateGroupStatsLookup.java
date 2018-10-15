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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;;

import java.util.Arrays;

public class CreateGroupStatsLookup implements Command {
    public final double[] stats;
    public final Optional<String> name;

    public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
        this.stats = stats;
        this.name = name;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws JsonProcessingException {
        final int depth = session.currentDepth;
        final double[] stats = this.stats;
        final Session.SavedGroupStats savedStats = new Session.SavedGroupStats(depth, stats);
        final String lookupName;
        if (this.name.isPresent()) {
            lookupName = this.name.get();
        } else {
            lookupName = String.valueOf(session.savedGroupStats.size());
        }
        if (session.savedGroupStats.containsKey(lookupName)) {
            throw new IllegalArgumentException("Name already in use!: [" + lookupName + "]");
        }
        session.savedGroupStats.put(lookupName, savedStats);
        out.accept(Session.MAPPER.writeValueAsString(Arrays.asList(lookupName)));
    }
}
