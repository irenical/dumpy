package org.irenical.dumpy;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.impl.ErrorJobProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.LatestJobProcessor;
import org.irenical.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Dumpy implements LifeCycle, Consumer< IJob > {

    private static final Logger LOGGER = LoggerFactory.getLogger( Dumpy.class );

    private final ExecutorService executorService = Executors.newFixedThreadPool( 2 );


    private final DumpyDB dumpyDB = new DumpyDB();

    private final IJobProcessor latestJobProcessor = new LatestJobProcessor( dumpyDB );

    private final IJobProcessor errorJobProcessor = new ErrorJobProcessor( dumpyDB );


    private boolean errorCheckEnabled = true;


    public Dumpy() {

    }

    public Dumpy(boolean errorCheckEnabled) {
        this.errorCheckEnabled = errorCheckEnabled;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        latestJobProcessor.start();
        errorJobProcessor.start();
        dumpyDB.start();
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        latestJobProcessor.stop();
        errorJobProcessor.stop();
        dumpyDB.stop();
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return latestJobProcessor.isRunning() && errorJobProcessor.isRunning() && dumpyDB.isRunning();
    }

    @Override
    public void accept(IJob iJob) {
        if ( ! isRunning() ) {
            throw new IllegalStateException( "dumpy is not started" );
        }

        executorService.execute(() -> latestJobProcessor.accept( iJob ));
        if ( errorCheckEnabled ) {
            executorService.execute(() -> errorJobProcessor.accept(iJob));
        }

//        do i need to do this? doesnt the job run until it finishes?
        executorService.shutdown();
        try {
            boolean awaitTermination = false;
            while ( ! awaitTermination ) {
                LOGGER.debug( "[ dumpy ] waiting for jobs to finish" );
                awaitTermination = executorService.awaitTermination( 10, TimeUnit.SECONDS );
            }
        } catch (InterruptedException e) {
            LOGGER.error( e.getLocalizedMessage(), e );
        }
    }

}
