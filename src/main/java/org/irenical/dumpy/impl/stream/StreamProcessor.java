package org.irenical.dumpy.impl.stream;

import org.irenical.dumpy.DumpyThreadFactory;
import org.irenical.dumpy.api.*;
import org.irenical.dumpy.impl.ExecutorTerminator;
import org.irenical.dumpy.impl.LoaderResponseHandler;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.model.DumpyBlockingQueue;
import org.irenical.jindy.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StreamProcessor implements IStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( StreamProcessor.class );

    private final ExecutorService loaderResponseExecutor = new ThreadPoolExecutor( 10, 10,
            0L, TimeUnit.MILLISECONDS,
            new DumpyBlockingQueue( 1000 ),
            new DumpyThreadFactory() );

    private final DumpyDB dumpyDB;

    private boolean isRunning = false;


    public StreamProcessor(DumpyDB dumpyDB) {
        this.dumpyDB = dumpyDB;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        isRunning = true;
    }

    @Override
    public void stop() throws Exception {
        isRunning = false;
        ExecutorTerminator.terminate( 10, Long.MAX_VALUE, loaderResponseExecutor );
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return isRunning && dumpyDB.isRunning() && ! loaderResponseExecutor.isTerminated();
    }

    /**
     * Producer/Consumer pattern, with 1 Producer N Consumers.
     */
    @Override
    public < TYPE, ERROR extends Exception > void process(IJob iJob, IStream< TYPE, ERROR > iStream) throws Exception {
        LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream start" );

        final IExtractor< TYPE, ERROR > iExtractor = iStream.getExtractor();
        final ILoader< TYPE > iLoader = iStream.getLoader();

        final int nThreads = ConfigFactory.getConfig().getInt("dumpy.latest.thread.count", 5);
        final int maxQueue = ConfigFactory.getConfig().getInt("dumpy.latest.thread.queue.capacity", 1000);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor( nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new DumpyBlockingQueue( maxQueue ),
                new DumpyThreadFactory() );

        try {
//            start by getting the current job/stream cursor (should get null for unknown job/stream)
            String cursor = dumpyDB.getCursor(iJob.getCode(), iStream.getCode());
            boolean hasNext = true;
            while ( isRunning() && hasNext ) {

//                get entities from extractor
                IExtractor.Response<TYPE> extractorResponse;
                List<IExtractor.Entity<TYPE>> entities;
                try {
                    extractorResponse = iExtractor.get(cursor);
                    entities = extractorResponse.getValues();
                } catch ( Exception e ) {
//                    any extractor error, skip it and keep trying
                    LOGGER.error( e.getLocalizedMessage(), e );
                    continue;
                }

                if ( isRunning() ) {

                    if (entities != null && !entities.isEmpty()) {
//                        send entities to the loader and handle its response (separate thread)
                        Future<Map< ? extends IExtractor.Entity< TYPE >, ILoader.Status>> loaderTask =
                                executorService.submit(() -> iLoader.load(entities));

                        loaderResponseExecutor.execute(new LoaderResponseHandler<>(dumpyDB, iJob, iStream, loaderTask));
                    }

//                    update next cursor iteration (if any)
                    cursor = extractorResponse.getCursor();
                    hasNext = extractorResponse.hasNext();
                    if ( extractorResponse.getCursor() != null ) {
                        // update next cursor
                        dumpyDB.setCursor(iJob.getCode(), iStream.getCode(), cursor);
                    }


//                    always sleep (free hardware resources)
                    String sleepValue = ConfigFactory.getConfig().getString("dumpy.latest.sleep", "1000");
                    LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] sleeping " + sleepValue + "ms" );
                    Thread.sleep( Long.valueOf( sleepValue ) );
                }

            }
        } finally {

            // wait for all tasks to complete
            ExecutorTerminator.terminate( 10, Long.MAX_VALUE, executorService );
            LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] stream done");

        }
    }

}
