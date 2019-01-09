package com.indeed.util.logging;

import io.opentracing.util.ThreadLocalActiveSpanSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.opentracing.mock.MockTracer;

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

    @Test
    public void testToString_operationNamesDontAffectTimingReport() {
        final TracingTreeTimer tracingTreeTimer = new TracingTreeTimer();
        tracingTreeTimer.push("opname1", "trace");
        tracingTreeTimer.push("opname2", "this");
        tracingTreeTimer.push("opname3", "tree");
        final String tracingTreeReport = tracingTreeTimer.toString();
        final String[] lines = tracingTreeReport.split("\n");
        assertTrue(lines[0].contains("trace"));
        assertTrue(lines[1].contains("this"));
        assertTrue(lines[2].contains("tree"));
    }

    @Test
    public void testTracing_operationNames() {
        final MockTracer mockTracer = new MockTracer(new ThreadLocalActiveSpanSource());
        final TracingTreeTimer tracingTreeTimer = new TracingTreeTimer(mockTracer);
        tracingTreeTimer.push("simple1");
        tracingTreeTimer.push("opname2", "detailed2");
        tracingTreeTimer.push("opname3", "detailed3");
        tracingTreeTimer.pop();
        tracingTreeTimer.pop();
        tracingTreeTimer.pop();
        assertEquals(3, mockTracer.finishedSpans().size());
        assertEquals("opname3", mockTracer.finishedSpans().get(0).operationName());
        assertEquals("detailed3", mockTracer.finishedSpans().get(0).tags().get("details"));
        assertEquals("opname2", mockTracer.finishedSpans().get(1).operationName());
        assertEquals("detailed2", mockTracer.finishedSpans().get(1).tags().get("details"));
        assertEquals("simple1", mockTracer.finishedSpans().get(2).operationName());
        assertFalse(mockTracer.finishedSpans().get(2).tags().containsKey("details"));
    }
}