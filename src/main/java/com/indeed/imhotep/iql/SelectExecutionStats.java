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

package com.indeed.imhotep.iql;

import com.google.common.collect.Sets;
import com.indeed.imhotep.api.PerformanceStats;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

import javax.annotation.Nullable;
import java.util.Set;

public class SelectExecutionStats {
    public boolean cached;
    public int rowsWritten;
    public boolean overflowedToDisk;
    public String hashForCaching;
    public long imhotepTempFilesBytesWritten;
    public int shardCount;
    public boolean headOnly;
    public String sessionId;
    public int maxImhotepGroups;
    public Object2LongMap<String> phases = new Object2LongArrayMap<String>();
    public long numDocs;
    @Nullable
    public PerformanceStats imhotepPerformanceStats;

    private final Set<String> phasesToReport = Sets.newHashSet("lockWaitMillis", "cacheCheckMillis", "shardsSelectionMillis");

    public SelectExecutionStats() {
        this.headOnly = false;
        hashForCaching = "";
        overflowedToDisk = false;
        rowsWritten = 0;
        cached = false;
        shardCount = 0;
        imhotepTempFilesBytesWritten = 0;
        maxImhotepGroups = 1;
        sessionId = "";
        numDocs = 0;
    }

    public void setPhase(final String phase, long elapsedtimemillis) {
        phases.put(phase, elapsedtimemillis);
    }

    public String getPhasesAsTimingReport() {
        final StringBuilder stringBuilder = new StringBuilder();
        for (Object2LongMap.Entry<String> entry : phases.object2LongEntrySet()) {
            if(!phasesToReport.contains(entry.getKey())) {
                continue;
            }
            stringBuilder.append(entry.getLongValue());
            stringBuilder.append("ms ");
            stringBuilder.append(entry.getKey());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
