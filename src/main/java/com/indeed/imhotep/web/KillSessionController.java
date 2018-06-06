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

package com.indeed.imhotep.web;

import com.google.common.collect.Lists;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.util.core.threads.NamedThreadFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author jsgroth
 */
@Controller
public class KillSessionController {
    private static final Logger log = Logger.getLogger(KillSessionController.class);

    private final ExecutorService threadPool;
    private final ImhotepClient imhotepClient;


    @Autowired
    public KillSessionController(ImhotepClient imhotepClient) {
        threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("IQL-Session-Killer"));
        this.imhotepClient = imhotepClient;
    }

    @RequestMapping("/killsession")
    protected void doGet(@RequestParam("session") final String sessionId, @RequestParam("username") final String username, final HttpServletResponse resp) throws IOException {
        final PrintWriter output = new PrintWriter(resp.getOutputStream());
        List<Host> hosts = imhotepClient.getServerHosts();

        boolean success = killSession(sessionId, hosts, output);
        if (success) {
            output.println("Session " + sessionId + " killed successfully");
            log.info("Session " + sessionId + " killed successfully by " + username);
        } else {
            log.info("Failed to close all sessions for " + sessionId + " requested by " + username);
        }
        output.flush();
    }

    // TODO: move this to ImhotepClient
    private boolean killSession(final String sessionId, List<Host> hosts, PrintWriter output) {
        final List<Callable<Throwable>> closers = Lists.newArrayList();
        for(final Host host : hosts) {
            final Callable<Throwable> callable = new Callable<Throwable>() {
                @Override
                public Throwable call() throws Exception {
                    try {
                        log.trace("Killing session " + sessionId + " on " + host.hostname + ":" + host.port);
                        ImhotepRemoteSession session = new ImhotepRemoteSession(host.hostname, host.port, sessionId, null);
                        session.close();
                    } catch (Throwable e) {
                        return e;
                    }
                    return null;
                }
            };
            closers.add(callable);

        }
        boolean success = true;
        try {
            List<Future<Throwable>> outcomes =
                    threadPool.invokeAll(closers, 1, TimeUnit.MINUTES);
            for (int i = 0; i < outcomes.size(); i++) {
                Throwable exception;
                try {
                    final Future<Throwable> outcome = outcomes.get(i);
                    //noinspection ThrowableResultOfMethodCallIgnored
                    exception = outcome.get();
                } catch (Exception e) {
                    exception = e;
                }
                if (exception != null) {
                    success = false;
                    final Host host = hosts.get(i);
                    if (output != null) {
                        output.println("Failed to close session on " + host.hostname + ":" + host.port + ":");
                        exception.printStackTrace(output);
                    }
                }
            }
        } catch (Exception e) {
            if (output != null) {
                output.println("Exception while closing the sessions:");
                e.printStackTrace(output);
            }
            success = false;
        }
        return success;
    }
}
