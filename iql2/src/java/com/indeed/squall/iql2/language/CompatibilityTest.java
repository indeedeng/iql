package com.indeed.squall.iql2.language;

import com.indeed.common.util.time.DefaultWallClock;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CompatibilityTest {
    private static final Logger log = Logger.getLogger(CompatibilityTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        log.setLevel(Level.TRACE);
        log.addAppender(new ConsoleAppender(new SimpleLayout()));

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
                    Queries.parseQuery(q, true, DatasetsMetadata.empty(), new DefaultWallClock());
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
