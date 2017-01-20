package org.irenical.dumpy;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.impl.job.ErrorJobProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.job.JobProcessor;
import org.irenical.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Dumpy implements LifeCycle, Consumer< IJob > {

    private static final Logger LOGGER = LoggerFactory.getLogger( Dumpy.class );

    private final ExecutorService executorService = Executors.newFixedThreadPool( 2 );


    private final DumpyDB dumpyDB = new DumpyDB();

    private final List< IJobProcessor > jobProcessors;


    public Dumpy() {
        this( true );
    }

    public Dumpy(boolean errorCheckEnabled) {
        this.jobProcessors = new LinkedList<>();
        this.jobProcessors.add( new JobProcessor( dumpyDB ) );
        if ( errorCheckEnabled ) {
            this.jobProcessors.add( new ErrorJobProcessor( dumpyDB ) );
        }
    }

    public Dumpy( List< IJobProcessor > jobProcessors ) {
        this.jobProcessors = jobProcessors;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        LOGGER.debug( "start() - starting up dumpy db ...");
        dumpyDB.start();

        LOGGER.debug( "start() - starting up job processors ..." );
        for (IJobProcessor jobProcessor : jobProcessors) {
            jobProcessor.start();
            LOGGER.debug( jobProcessor.getClass().getName() + " started." );
        }
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        LOGGER.debug( "stop() - stopping job processors ..." );
        for (IJobProcessor jobProcessor : jobProcessors) {
            jobProcessor.stop();
            LOGGER.debug( jobProcessor.getClass().getName() + " stopped." );
        }

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

        LOGGER.debug( "stop() - stopping dumpy db ...");
        dumpyDB.stop();
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return dumpyDB.isRunning() && jobProcessors.stream().allMatch(LifeCycle::isRunning);
    }

    @Override
    public void accept(IJob iJob) {
        if ( ! isRunning() ) {
            throw new IllegalStateException( "dumpy is not started" );
        }

        LOGGER.debug( "accepted " + iJob.getCode() + ": processing ..." );
        for (IJobProcessor jobProcessor : jobProcessors) {
            executorService.execute( () -> jobProcessor.accept( iJob ) );
        }
    }

}
