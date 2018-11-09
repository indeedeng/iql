package com.indeed.iql.cache;

import java.io.IOException;
import java.io.OutputStream;

public class DelegatingCompletableOutputStream extends CompletableOutputStream {
    private final OutputStream outputStream;

    public DelegatingCompletableOutputStream(final OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(final int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
