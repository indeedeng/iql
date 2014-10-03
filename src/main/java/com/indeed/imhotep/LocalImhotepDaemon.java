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
