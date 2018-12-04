package com.indeed.util.logging;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TracingTreeTimerTest {
    @Test
    public void testPopping_emptyAfterClose() {
        final TracingTreeTimer tracingTreeTimer = new TracingTreeTimer();
        tracingTreeTimer.push("trace");
        tracingTreeTimer.push("this");
        tracingTreeTimer.push("tree");
        tracingTreeTimer.close();
        assertEquals(-1, tracingTreeTimer.pop());
    }

    @Test
    public void testPopping_notEmptyIfDontClose() {
        final TracingTreeTimer tracingTreeTimer = new TracingTreeTimer();
        tracingTreeTimer.push("trace");
        tracingTreeTimer.push("another");
        tracingTreeTimer.push("tree");
        assertNotSame(-1, tracingTreeTimer.pop());
        assertNotSame(-1, tracingTreeTimer.pop());
        assertNotSame(-1, tracingTreeTimer.pop());
        assertEquals(-1, tracingTreeTimer.pop());
    }

    @Test
    public void testToString() {
        final TracingTreeTimer tracingTreeTimer = new TracingTreeTimer();
        tracingTreeTimer.push("trace");
        tracingTreeTimer.push("this");
        tracingTreeTimer.push("tree");
        final String tracingTreeReport = tracingTreeTimer.toString();
        final String[] lines = tracingTreeReport.split("\n");
        assertTrue(lines[0].contains("trace"));
        assertTrue(lines[1].contains("this"));
        assertTrue(lines[2].contains("tree"));
    }
}