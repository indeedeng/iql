/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.util.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

// TODO: move to https://github.com/indeedeng/util/tree/master/util-core/src/main/java/com/indeed/util/core

/**
 * Small wrapper class around common logging patterns
 * <p/>
 * The main pattern I want to pull out of the main codebase are using String.format() (or string math) to construct
 * detailed log messages inside isDebugEnabled() conditionals.
 */
public class Loggers {
    public static void trace(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace(message);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    public static void trace(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace(message, t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void trace(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace(String.format(template, arguments));

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void trace(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace(String.format(template, arguments), t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    public static void debug(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        if (logger.isDebugEnabled()) {
            try {
                logger.debug(message);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    public static void debug(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        if (logger.isDebugEnabled()) {
            try {
                logger.debug(message, t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void debug(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        if (logger.isDebugEnabled()) {
            try {
                logger.debug(String.format(template, arguments));

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void debug(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        if (logger.isDebugEnabled()) {
            try {
                logger.debug(String.format(template, arguments), t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    public static void info(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        if (logger.isInfoEnabled()) {
            try {
                logger.info(message);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    public static void info(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        if (logger.isInfoEnabled()) {
            try {
                logger.info(message, t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void info(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        if (logger.isInfoEnabled()) {
            try {
                logger.info(String.format(template, arguments));

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void info(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        if (logger.isInfoEnabled()) {
            try {
                logger.info(String.format(template, arguments), t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    public static void warn(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        if (logger.isEnabledFor(Level.WARN)) {
            try {
                logger.warn(message);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    public static void warn(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        if (logger.isEnabledFor(Level.WARN)) {
            try {
                logger.warn(message, t);
            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));

            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void warn(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        if (logger.isEnabledFor(Level.WARN)) {
            try {
                logger.warn(String.format(template, arguments));
            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));

            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void warn(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        if (logger.isEnabledFor(Level.WARN)) {
            try {
                logger.warn(String.format(template, arguments), t);
            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));

            }
        }
    }

    public static void error(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        if (logger.isEnabledFor(Level.ERROR)) {
            try {
                logger.error(message);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    public static void error(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        if (logger.isEnabledFor(Level.ERROR)) {
            try {
                logger.error(message, t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(message, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void error(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        if (logger.isEnabledFor(Level.ERROR)) {
            try {
                logger.error(String.format(template, arguments));

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log. Message formatting will <b>not</b> occur if the given <code>logger</code> has
     *  a higher effective level.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void error(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        if (logger.isEnabledFor(Level.ERROR)) {
            try {
                logger.error(String.format(template, arguments), t);

            } catch (final Exception e) {
                logger.error("Unable to format log message", new LogFormatException(template, e));
            }
        }
    }


    public static void fatal(
            @Nonnull final Logger logger,
            @Nonnull final String message
    ) {
        try {
            logger.fatal(message);

        } catch (final Exception e) {
            logger.error("Unable to format log message", new LogFormatException(message, e));
        }
    }

    public static void fatal(
            @Nonnull final Logger logger,
            @Nonnull final String message,
            @Nonnull final Throwable t
    ) {
        try {
            logger.fatal(message, t);

        } catch (final Exception e) {
            logger.error("Unable to format log message", new LogFormatException(message, e));
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param arguments
     */
    public static void fatal(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Object... arguments
    ) {
        try {
            logger.fatal(String.format(template, arguments));

        } catch (final Exception e) {
            logger.error("Unable to format log message", new LogFormatException(template, e));
        }
    }

    /**
     * Use {@link String#format} to populate the given <code>template</code> with the given <code>arguments</code>
     *  and write the result to log.
     * <p>
     * Caller is responsible for ensuring that the arguments list matches the number and type requirements of
     *  the template.
     *
     * @param logger
     * @param template https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#syntax
     * @param t
     * @param arguments
     */
    public static void fatal(
            @Nonnull final Logger logger,
            @Nonnull final String template,
            @Nonnull final Throwable t,
            @Nonnull final Object... arguments
    ) {
        try {
            logger.fatal(String.format(template, arguments), t);

        } catch (final Exception e) {
            logger.error("Unable to format log message", new LogFormatException(template, e));
        }
    }

    public static class LogFormatException extends RuntimeException {
        public LogFormatException(@Nonnull final String messageOrTemplate, @Nonnull final Throwable rootCause) {
            super("Failed to format log message: '" + messageOrTemplate + "'.", rootCause);
        }
    }
}
