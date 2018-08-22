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
package com.indeed.iql;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.indeed.imhotep.service.GenericFlamdexReaderSource;
import com.indeed.imhotep.service.ImhotepDaemon;
import com.indeed.imhotep.service.LocalImhotepServiceConfig;
import com.indeed.imhotep.service.LocalImhotepServiceCore;
import com.indeed.imhotep.shardmaster.ShardFilter;
import com.indeed.imhotep.shardmaster.ShardMasterDaemon;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;

/**
 * @author vladimir
 */

public class LocalImhotepDaemonAndShardmaster {
    private static final Logger log = Logger.getLogger(LocalImhotepDaemonAndShardmaster.class);

    /**
     * Starts an embedded instance of ImhotepDaemon and local ShardMaster
     * @return a pair where the first element is the shardmaster port and the second is the imhotep daemon port
     */
    public static Pair<Integer, Integer> startInProcess(String shardsDir) {
        try {
            final ServerSocket ss = new ServerSocket(0);
            final int imhotepPort = ss.getLocalPort();
            final String tempDir = Files.createTempDir().getAbsolutePath();
            final ImhotepDaemon theImhotep = new ImhotepDaemon(
                    ss,
                    new LocalImhotepServiceCore(Paths.get(tempDir), Long.MAX_VALUE, new GenericFlamdexReaderSource(), new LocalImhotepServiceConfig(), Paths.get(shardsDir)),
                    null,
                    null,
                    "localhost",
                    imhotepPort,
                    null
            );
            new Thread(theImhotep::run).start();
            theImhotep.waitForStartup(30000L);
            log.info("Local Imhotep Daemon instance started on port " + imhotepPort);

            ServerSocket shardMasterSocket = new ServerSocket(0);
            final ShardMasterDaemon shardMasterDaemon = new ShardMasterDaemon(new ShardMasterDaemon.Config()
                    .setLocalMode(true)
                    .setHostsListStatic("localhost:"+imhotepPort)
                    .setShardsRootPath(shardsDir)
                    .setServerSocket(shardMasterSocket));
            new Thread(() -> {
                try {
                    shardMasterDaemon.run();
                } catch (IOException | InterruptedException | KeeperException e) {
                    log.error("Could not create ShardMaster", e);
                }
            }).start();
            int shardMasterPort = shardMasterSocket.getLocalPort();

            shardMasterDaemon.waitForStartup(300000L);
            log.info("Local ShardMaster instance started on port " + shardMasterPort);

            return new Pair<>(shardMasterPort, imhotepPort);
        } catch(Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
