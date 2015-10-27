package com.indeed.imhotep.iql;

public class SelectExecutionStats {
    public boolean cached;
    public int rowsWritten;
    public boolean overflowedToDisk;
    public String hashForCaching;
    public long imhotepTempFilesBytesWritten;
    public int shardCount;
    public boolean headOnly;
    public String sessionId;

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
}