package org.irenical.dumpy.impl.stream;

import org.irenical.dumpy.DumpyThreadFactory;
import org.irenical.dumpy.api.*;
import org.irenical.dumpy.impl.ExecutorTerminator;
import org.irenical.dumpy.impl.LoaderResponseHandler;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.model.DumpyBlockingQueue;
import org.irenical.dumpy.impl.model.PaginatedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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
    public void stop() throws Exception {
        isRunning = false;
        ExecutorTerminator.terminate( 10, Long.MAX_VALUE, loaderResponseExecutor );
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

//        TODO : magic number
        final int nThreads = 5;
        final int maxQueue = 1000;
        ExecutorService executorService = new ThreadPoolExecutor( nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new DumpyBlockingQueue( maxQueue ),
                new DumpyThreadFactory() );

        try {
//            control dumpy own cursor
            String cursor = null;
            boolean hasNext = true;

            while ( isRunning() && hasNext ) {
//                LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] iteration" );

//                get errored entities for this stream from the db
                PaginatedResponse<String> response = dumpyDB.get(iJob.getCode(), iStream.getCode(), cursor);
                List<String> values = response.getValues();
//                LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] db: " + response.getValues().size() );

//                no values to try again? continue
                if (values != null && !values.isEmpty()) {
//                    control extractor cursor
                    String extractorCursor = null;
                    boolean extractorHasNext = true;
                    while ( isRunning() && extractorHasNext ) {
//                        LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] cursor=" + cursor);

//                        get those entities from stream extractor ( by entityId )
                        IExtractor.Response<TYPE> typeResponse = iExtractor.get(response.values, extractorCursor);
                        List<IExtractor.Entity<TYPE>> entities = typeResponse.getValues();
//                        LOGGER.debug("[ processor( " + iStream.getCode() + " ) ] extractor: " + typeResponse.getValues().size());

                        if ( isRunning() && entities != null && !entities.isEmpty()) {
//                            load entities and process response
                            Future<Map< ? extends IExtractor.Entity< TYPE >, ILoader.Status>> loaderTask =
                                    executorService.submit(() -> iLoader.load(entities));

                            loaderResponseExecutor.execute(new LoaderResponseHandler<>(dumpyDB, iJob, iStream, loaderTask));
                        }

//                        update extractor cursor
                        extractorCursor = typeResponse.getCursor();
                        extractorHasNext = typeResponse.hasNext();
                    }
                }

//                update dumpy cursor
                cursor = response.cursor;
                hasNext = response.hasNext;

                if ( values == null || values.isEmpty() ) {
                    Thread.sleep( 1000L );
                }

            }
        } finally {

            // wait for all tasks to complete
            ExecutorTerminator.terminate( 10, Long.MAX_VALUE, executorService );
            LOGGER.debug( "[ processor( " + iStream.getCode() + " ) ] stream done" );

        }

    }

}
