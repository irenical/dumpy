package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.jindy.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.irenical.dumpy.DumpyThreadFactory;
import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.api.IStreamProcessor;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StreamProcessor implements IStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( StreamProcessor.class );

    private static final int DEFAULT_AWAIT_TERMINATION_TIMEOUT_SECONDS = 60;

    private int awaitTerminationTimeoutSeconds;

    private final DumpyDB dumpyDB;

    private final ExecutorService loaderResponseExecutor = Executors.newCachedThreadPool( new DumpyThreadFactory() );

    private boolean isRunning = false;


    public StreamProcessor( DumpyDB dumpyDB ) {
        this.dumpyDB = dumpyDB;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        awaitTerminationTimeoutSeconds = ConfigFactory.getConfig().getInt( "streamprocessor.await-timeout-seconds",
                DEFAULT_AWAIT_TERMINATION_TIMEOUT_SECONDS );
        isRunning = true;
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        isRunning = false;
        loaderResponseExecutor.shutdown();
        try {
            loaderResponseExecutor.awaitTermination( Long.MAX_VALUE, TimeUnit.NANOSECONDS );
        } catch (InterruptedException e) {
            LOGGER.error( e.getLocalizedMessage(), e );
        }
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return isRunning && dumpyDB.isRunning() && ! loaderResponseExecutor.isTerminated();
    }

    /**
     * Producer/Consumer pattern, with 1 Producer and N Consumers.
     * There is no need to add complexity of multiple executor services for producer/consumer,
     * as there is only one producer, submiting directly to consumers
     */
    @Override
    public < TYPE > void process(IJob iJob, IStream< TYPE > iStream) throws Exception {
        LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream start" );

        final IExtractor< TYPE > iExtractor = iStream.getExtractor();
        final ILoader< TYPE > iLoader = iStream.getLoader();


        ExecutorService executorService = Executors.newCachedThreadPool( new DumpyThreadFactory() );

        String cursor = dumpyDB.getCursor( iJob.getCode(), iStream.getCode() );
        boolean hasNext = true;
        while ( isRunning && hasNext ) {
            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] cursor=" + cursor );
            IExtractor.Response< TYPE > extractorResponse = iExtractor.get(cursor);

            Future<ILoader.Status> loaderTask = executorService.submit( () ->
                    iLoader.load(extractorResponse.getEntities()) );

            loaderResponseExecutor.execute( new LoaderResponseHandler<>( dumpyDB, iJob, iStream, loaderTask,
                    new LinkedList<IExtractor.Entity<TYPE>>( extractorResponse.getEntities() ) ) );

            // update next cursor
            dumpyDB.setCursor( iJob.getCode(), iStream.getCode(), cursor );

            cursor = extractorResponse.getCursor();
            hasNext = extractorResponse.hasNext();
        }

        // wait for all tasks to complete
        executorService.shutdown();
        try {
            executorService.awaitTermination( awaitTerminationTimeoutSeconds, TimeUnit.SECONDS );
            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream done" );
        } catch ( InterruptedException e ) {
            LOGGER.error( e.getLocalizedMessage(), e );
            executorService.shutdownNow();
            executorService.awaitTermination( awaitTerminationTimeoutSeconds, TimeUnit.SECONDS );
        }
    }

}
