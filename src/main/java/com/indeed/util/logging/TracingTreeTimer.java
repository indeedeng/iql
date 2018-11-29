package com.indeed.util.logging;

import com.indeed.util.core.TreeTimer;
import io.opentracing.ActiveSpan;
import io.opentracing.util.GlobalTracer;

import java.util.Stack;

/**
 * Operates the same way as {@link com.indeed.util.core.TreeTimer}, but also creates OpenTracing spans.
 */
public class TracingTreeTimer implements AutoCloseable {
    private final TreeTimer treeTimer;
    private final Stack<ActiveSpan> activeSpanStack = new Stack<>();

    public TracingTreeTimer() {
        treeTimer = new TreeTimer();
    }

    public void push(String s) {
        final ActiveSpan activeSpan = GlobalTracer.get().buildSpan(s).startActive();
        activeSpanStack.push(activeSpan);
        treeTimer.push(s);
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

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void close() {
        while (pop() != -1);
    }
}
