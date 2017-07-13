package com.indeed.squall.iql2.server.web;

/**
 * @author vladimir
 */

public class GlobalUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    static final int RETURN_CODE = 2;
    static final int SHUTDOWN_ALLOW_TIME = 60000;   // allow 1 minute to cleanly exit

    public void uncaughtException(Thread t, Throwable e) {
        if(e instanceof OutOfMemoryError) {
            try {
                System.err.println("Out Of Memory encountered in a non Query Servlet thread. Can't recover.");
                e.printStackTrace(System.err);
            } catch (Throwable ignored)  {}

            terminate(RETURN_CODE, SHUTDOWN_ALLOW_TIME);
        }
    }

    public static void terminate(int returnCode, final int shutdownTimeoutMS) {
        try {
            ExitThread exitThread = new ExitThread(returnCode);
            exitThread.start();
            exitThread.join(shutdownTimeoutMS);
            if (!exitThread.finished) {
                try {
                    System.err.println("Shutdown timed out. Force halting VM");
                } finally {
                    Runtime.getRuntime().halt(returnCode);  // we couldn't shut down in time. force stop
                }
            }

        } catch (Throwable ignored) {
            try {
                System.err.println("Error during shut down. Force halting VM");
            } finally {
                Runtime.getRuntime().halt(returnCode); // we had a problem during orderly shutdown. force stop
            }
        }
    }

    private static class ExitThread extends Thread {
        final int returnCode;
        boolean finished = false;

        private ExitThread(int returnCode) {
            this.returnCode = returnCode;
        }

        public void run() {
            System.exit(returnCode);
            finished = true;
            System.err.println("Completed orderly shut down");  // this doesn't actually get printed
        }
    }

    /**
     * Registers itself as the default uncaught exception handler
     */
    public static void register() {
        Thread.setDefaultUncaughtExceptionHandler(new GlobalUncaughtExceptionHandler());
    }
}
