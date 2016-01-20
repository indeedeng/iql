package com.indeed.imhotep.iql;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

public class SelectExecutionStats {
    public boolean cached;
    public int rowsWritten;
    public boolean overflowedToDisk;
    public String hashForCaching;
    public long imhotepTempFilesBytesWritten;
    public int shardCount;
    public boolean headOnly;
    public String sessionId;
    public Object2LongMap<String> phases = new Object2LongArrayMap<String>();

    public SelectExecutionStats() {
        this.headOnly = false;
        hashForCaching = "";
        overflowedToDisk = false;
        rowsWritten = 0;
        cached = false;
        shardCount = 0;
        imhotepTempFilesBytesWritten = 0;
        sessionId = "";
    }

    public void setPhase(final String phase, long elapsedtimemillis) {
        phases.put(phase, elapsedtimemillis);
    }
}
