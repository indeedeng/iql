package com.indeed.util.logging;

import com.indeed.util.core.TreeTimer;
import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import java.util.Stack;

/**
 * Operates the same way as {@link com.indeed.util.core.TreeTimer}, but also creates OpenTracing spans.
 */
public class TracingTreeTimer implements AutoCloseable {
    private final TreeTimer treeTimer;
    private final Stack<ActiveSpan> activeSpanStack = new Stack<>();
    private final Tracer tracer;

    public TracingTreeTimer(final Tracer tracer) {
        this.tracer = tracer;
        treeTimer = new TreeTimer();
    }

    public TracingTreeTimer() {
        this(GlobalTracer.get());
    }

    public void push(String s) {
        push(s, s);
    }

    public void push(String operationName, String detailedString) {
        final Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName);
        if (!operationName.equals(detailedString)) {
            spanBuilder.withTag("details", detailedString);
        }
        final ActiveSpan activeSpan = spanBuilder.startActive();
        activeSpanStack.push(activeSpan);
        treeTimer.push(detailedString);
    }

    public int pop() {
        if (!activeSpanStack.isEmpty()) {
            final ActiveSpan activeSpan = activeSpanStack.pop();
            activeSpan.close();
            return treeTimer.pop();
        }
        return -1;
    }

    @Override
    public String toString() {
        return treeTimer.toString();
    }

    @Override
    public void close() {
        while (true) {
            if (pop() == -1) {
                return;
            }
        }
    }
}
