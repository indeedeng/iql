package com.indeed.imhotep.iql;

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

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
