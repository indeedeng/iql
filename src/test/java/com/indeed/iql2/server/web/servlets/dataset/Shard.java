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

package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;

import java.io.IOException;

public class Shard {
    public final String dataset;
    public final String shardId;
    public final MemoryFlamdex flamdex;

    public Shard(String dataset, String shardId, MemoryFlamdex flamdex) {
        this.dataset = dataset;
        this.shardId = shardId;
        this.flamdex = flamdex;
    }

    public void addTo(ShardMasterAndImhotepDaemonClusterRunner clusterRunner) throws IOException {
        clusterRunner.createShardUnversioned(dataset, shardId, flamdex);
    }

    @Override
    public String toString() {
        return "Shard{" +
                "dataset='" + dataset + '\'' +
                ", shardId='" + shardId + '\'' +
                ", flamdex=" + flamdex +
                '}';
    }
}
