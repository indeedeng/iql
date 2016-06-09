package com.indeed.imhotep.iql;

/**
 * @author vladimir
 */

public class LimitExceededException extends RuntimeException {
    /**
     * Constructs an <code>LimitExceededException</code> with no
     * detail message.
     */
    public LimitExceededException() {
        super();
    }

    /**
     * Constructs an <code>LimitExceededException</code> with the
     * specified detail message.
     *
     * @param   s   the detail message.
     */
    public LimitExceededException(String s) {
        super(s);
    }

    /**
     * Constructs a new exception with the specified detail message and
     * cause.
     *
     * <p>Note that the detail message associated with <code>cause</code> is
     * <i>not</i> automatically incorporated in this exception's detail
     * message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link Throwable#getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link Throwable#getCause()} method).  (A <tt>null</tt> value
     *         is permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public LimitExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
