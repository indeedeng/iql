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

package com.indeed.iql2.language;

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;

public class CompatibilityTest {
    private CompatibilityTest() {
    }

    private static final Logger log = Logger.getLogger(CompatibilityTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        log.setLevel(Level.TRACE);
        log.addAppender(new ConsoleAppender(new SimpleLayout()));

        final DefaultWallClock clock = new DefaultWallClock();
        final TracingTreeTimer timer = new TracingTreeTimer();
        final ShardResolver shardResolver = new NullShardResolver();

        final String path = args[0];
        int successes = 0;
        int failures = 0;
        try (final BufferedReader r = new BufferedReader(new FileReader(path))) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                final String[] split = line.split("\t");
                final String q = split[0];
                if (q.startsWith("desc")) {
                    continue;
                }
                successes++;
                try {
                    Queries.parseQuery(q, true, DatasetsMetadata.empty(), Collections.emptySet(), clock, timer, shardResolver);
                } catch (Exception e) {
                    successes--;
                    failures++;
                    log.error("q = " + q, e);
                }
            }
        }
        System.out.println("successes = " + successes);
        System.out.println("failures = " + failures);
    }
}
