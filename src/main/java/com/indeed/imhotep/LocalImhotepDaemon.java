/*
 * Copyright (C) 2014 Indeed Inc.
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
package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.indeed.imhotep.service.GenericFlamdexReaderSource;
import com.indeed.imhotep.service.ImhotepDaemon;
import com.indeed.imhotep.service.LocalImhotepServiceConfig;
import com.indeed.imhotep.service.LocalImhotepServiceCore;
import org.apache.log4j.Logger;

import java.net.ServerSocket;

/**
 * @author vladimir
 */

public class LocalImhotepDaemon {
    private static final Logger log = Logger.getLogger(LocalImhotepDaemon.class);

    /**
     * Starts an embedded instance of ImhotepDaemon and returns the port it is running on.
     */
    public static int startInProcess(String shardsDir) {
        try {
            final ServerSocket ss = new ServerSocket(0);
            final int imhotepPort = ss.getLocalPort();
            final String tempDir = Files.createTempDir().getAbsolutePath();
            final ImhotepDaemon theImhotep = new ImhotepDaemon(
                    ss,
                    new LocalImhotepServiceCore(shardsDir, tempDir, Long.MAX_VALUE, false, new GenericFlamdexReaderSource(), new LocalImhotepServiceConfig()),
                    null,
                    null,
                    "localhost",
                    imhotepPort
            );
            new Thread(new Runnable() {
                @Override
                public void run() {
                    theImhotep.run();
                }
            }).start();
            theImhotep.waitForStartup(30000L);
            log.info("Local Imhotep Daemon instance started on port " + imhotepPort);
            return imhotepPort;
        } catch(Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
