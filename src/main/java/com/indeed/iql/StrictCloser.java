package com.indeed.iql;

import com.google.common.io.Closer;
import com.indeed.iql1.iql.IQLQuery;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

/**
 * Similar to com.google.common.io.Closer, but once it has been closed once,
 * any subsequently registered {@code Closeable} should be instantly closed.
 */
public class StrictCloser implements Closeable {

    private static final Logger log = Logger.getLogger(StrictCloser.class);

    private final Closer closer = Closer.create();
    private boolean closed = false;

    @Override
    public void close() throws IOException {
        boolean needsClose = false;
        synchronized (this) {
            if (!closed) {
                closed = true;
                needsClose = true;
            }
        }
        if (needsClose) {
            closer.close();
        }
    }

    /**
     * Registers the given {@code closeable} to be closed when this {@code StrictCloser} is
     * {@linkplain #close closed}.
     *
     * If this {@code StrictCloser} is already closed, quietly close the given {@code closeable}
     *
     * @return the given {@code closeable}
     */
    public <C extends Closeable> C register(@Nullable C closeable) {
        boolean wasClosed = false;
        synchronized (this) {
            if (!closed) {
                closer.register(closeable);
            } else {
                wasClosed = true;
            }
        }
        if (wasClosed && closeable != null) {
            Closeables2.closeQuietly(closeable, log);
        }
        return closeable;
    }

    public boolean isClosed() {
        return closed;
    }
}
