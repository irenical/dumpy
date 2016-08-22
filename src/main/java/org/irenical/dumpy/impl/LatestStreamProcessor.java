package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.impl.db.DumpyDB;
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

public class LatestStreamProcessor implements IStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( LatestStreamProcessor.class );

    private final ExecutorService loaderResponseExecutor = Executors.newCachedThreadPool( new DumpyThreadFactory() );

    private final DumpyDB dumpyDB;

    private boolean isRunning = false;


    public LatestStreamProcessor(DumpyDB dumpyDB ) {
        this.dumpyDB = dumpyDB;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
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
    public < TYPE, ERROR extends Exception > void process(IJob iJob, IStream< TYPE, ERROR > iStream) throws Exception {
        LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream start" );

        final IExtractor< TYPE, ERROR > iExtractor = iStream.getExtractor();
        final ILoader< TYPE > iLoader = iStream.getLoader();

        ExecutorService executorService = Executors.newCachedThreadPool( new DumpyThreadFactory() );

        try {
            String cursor = dumpyDB.getCursor(iJob.getCode(), iStream.getCode());
            boolean hasNext = true;
            while (isRunning && hasNext) {
                //            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] cursor=" + cursor );
                IExtractor.Response<TYPE> extractorResponse = iExtractor.get(cursor);

                if (extractorResponse.getValues() != null && !extractorResponse.getValues().isEmpty()) {
                    Future<ILoader.Status> loaderTask = executorService.submit(() ->
                            iLoader.load(extractorResponse.getValues()));

                    loaderResponseExecutor.execute(new LoaderResponseHandler<>(dumpyDB, iJob, iStream, loaderTask,
                            new LinkedList<>(extractorResponse.getValues())));
                }

                // update next cursor
                dumpyDB.setCursor(iJob.getCode(), iStream.getCode(), cursor);

                cursor = extractorResponse.getCursor();
                hasNext = extractorResponse.hasNext();
            }
        } finally {

            // wait for all tasks to complete
            executorService.shutdown();
            try {
                boolean awaitTermination = false;
                while (!awaitTermination) {
                    LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] waiting for loaders to finish" );
                    awaitTermination = executorService.awaitTermination(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                LOGGER.error(e.getLocalizedMessage(), e);
                executorService.shutdownNow();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
            LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] stream done");

        }
    }

}
