package org.irenical.dumpy.impl;

import org.irenical.dumpy.DumpyThreadFactory;
import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.api.IStreamProcessor;
import org.irenical.dumpy.impl.model.PaginatedResponse;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ErrorStreamProcessor implements IStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( ErrorStreamProcessor.class );

    private final DumpyDB dumpyDB;

    private boolean isRunning = false;

    private final ExecutorService loaderResponseExecutor = Executors.newCachedThreadPool( new DumpyThreadFactory() );


    public ErrorStreamProcessor( DumpyDB dumpyDB ) {
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

    @Override
    public <TYPE, ERROR extends Exception> void process(IJob iJob, IStream<TYPE, ERROR> iStream) throws Exception {
        LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream start" );

        final IExtractor< TYPE, ERROR > iExtractor = iStream.getExtractor();
        final ILoader< TYPE > iLoader = iStream.getLoader();

        ExecutorService executorService = Executors.newCachedThreadPool( new DumpyThreadFactory() );


//        control dumpy own cursor
        String cursor = null;
        boolean hasNext = true;

        while ( isRunning && hasNext ) {
//            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] iteration" );

            // get errored entities for this stream from the db
            PaginatedResponse<String> response = dumpyDB.get( iJob.getCode(), iStream.getCode(), cursor );
//            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] db: " + response.getValues().size() );

//            not values to try again? continue
            if ( response.getValues() != null && ! response.getValues().isEmpty() ) {
//              control extractor cursor
                String extractorCursor = null;
                boolean extractorHasNext = true;
                while (extractorHasNext) {
//                    LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] cursor=" + cursor);

                    // get those entities from stream extractor ( by entityId )
                    IExtractor.Response<TYPE> typeResponse = iExtractor.get(response.values, extractorCursor);
//                    LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] extractor: " + typeResponse.getValues().size());

                    if ( typeResponse.getValues() != null && ! typeResponse.getValues().isEmpty() ) {
                        // load entities and process response
                        Future<ILoader.Status> loaderTask = executorService.submit(() ->
                                iLoader.load(typeResponse.getValues()));

                        loaderResponseExecutor.execute(new LoaderResponseHandler<>(dumpyDB, iJob, iStream, loaderTask,
                                new LinkedList<>(typeResponse.getValues())));
                    }

                    //                update extractor cursor
                    extractorCursor = typeResponse.getCursor();
                    extractorHasNext = typeResponse.hasNext();
                }
            }

//            update dumpy cursor
            cursor = response.cursor;
            hasNext = response.hasNext;
        }

        // wait for all tasks to complete
        executorService.shutdown();
        try {
            boolean awaitTermination = false;
            while ( ! awaitTermination ) {
                awaitTermination = executorService.awaitTermination( 10, TimeUnit.SECONDS );
            }
        } catch ( InterruptedException e ) {
            LOGGER.error( e.getLocalizedMessage(), e );
            executorService.shutdownNow();
            executorService.awaitTermination( Long.MAX_VALUE, TimeUnit.NANOSECONDS );
        }

        LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream done" );
    }

}
