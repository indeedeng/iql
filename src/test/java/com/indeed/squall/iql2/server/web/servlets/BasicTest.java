package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.squall.iql2.execution.metrics.aggregate.Log;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * @author zheli
 */
@Ignore
public abstract class BasicTest {
    public static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);
    private static Level level;

    @BeforeClass public static void disbaleVarExporterWarning() {
        level = Logger.getLogger(VarExporter.class).getLevel();
        Logger.getLogger(VarExporter.class).setLevel(Level.OFF);
    }

    @AfterClass public static void enableVarExporter() {
        Logger.getLogger(VarExporter.class).setLevel(level);
    }
}
