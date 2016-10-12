package org.irenical.dumpy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorTerminator {

    private static final Logger LOGGER = LoggerFactory.getLogger( ExecutorTerminator.class );

    private ExecutorTerminator() {

    }

    public static void terminate( long timeout, long interruptedTimeout, ExecutorService executorService ) throws InterruptedException {
        executorService.shutdown();
        try {
            boolean awaitTermination = false;
            while (!awaitTermination) {
                LOGGER.debug( "[ processor( onStop() ) ] waiting for loader handlers to finish ..." );
                awaitTermination = executorService.awaitTermination( timeout, TimeUnit.SECONDS );
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            executorService.shutdownNow();
            executorService.awaitTermination( interruptedTimeout, TimeUnit.NANOSECONDS );
        }
    }

}
