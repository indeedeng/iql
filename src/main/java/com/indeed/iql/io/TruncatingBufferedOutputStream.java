package com.indeed.iql.io;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A BufferedOutputStream which will continue to silently accept (but not write!) any
 * additional bytes it receives after passing the maximum number of bytes to write.
 *
 * If you somehow need to write more than Long.MAX_VALUE bytes, then you should update
 * the logic of this method to support it.
 */
public class TruncatingBufferedOutputStream extends BufferedOutputStream {
    private long remainingBytes;
    private boolean overflowed = false;

    public TruncatingBufferedOutputStream(final OutputStream out, @Nullable final Long maxBytesToWrite) {
        this(out, maxBytesToWrite, 8192);
    }

    public TruncatingBufferedOutputStream(final OutputStream out, @Nullable final Long maxBytesToWrite, final int bufferSize) {
        super(out, bufferSize);
        this.remainingBytes = (maxBytesToWrite == null) ? Long.MAX_VALUE : maxBytesToWrite;
    }

    public boolean isOverflowed() {
        return overflowed;
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        if (overflowed) {
            return;
        }

        remainingBytes -= 1;
        if (remainingBytes < 0L) {
            overflowed = true;
        } else {
            super.write(b);
        }
    }

    @Override
    public synchronized void write(final byte[] b, final int off, final int len) throws IOException {
        if (overflowed) {
            return;
        }

        remainingBytes -= len;
        if (remainingBytes < 0L) {
            overflowed = true;
        } else {
            super.write(b, off, len);
        }
    }
}
