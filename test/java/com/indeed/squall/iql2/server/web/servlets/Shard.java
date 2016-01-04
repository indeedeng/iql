package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.flamdex.MemoryFlamdex;

public class Shard {
    public final String dataset;
    public final String shardId;
    public final MemoryFlamdex flamdex;

    public Shard(String dataset, String shardId, MemoryFlamdex flamdex) {
        this.dataset = dataset;
        this.shardId = shardId;
        this.flamdex = flamdex;
    }
}
