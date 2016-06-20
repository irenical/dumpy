package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.api.IStreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class BaseJobProcessor implements IJobProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( LatestJobProcessor.class );

    private final IStreamProcessor streamProcessor;

    private boolean isRunning = false;

    protected final DumpyDB dumpyDB;

    public BaseJobProcessor(DumpyDB dumpyDB, IStreamProcessor streamProcessor ) {
        this.dumpyDB = dumpyDB;
        this.streamProcessor = streamProcessor;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        streamProcessor.start();
        isRunning = true;
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        isRunning = false;
        streamProcessor.stop();
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return isRunning && dumpyDB.isRunning() && streamProcessor.isRunning();
    }

    @Override
    public void accept(IJob iJob) throws SQLException {
        if ( iJob == null ) {
            throw new IllegalArgumentException( "no job provided" );
        }

        List<IStream> streams = iJob.getStreams();
        if ( streams == null || streams.isEmpty() ) {
            throw new IllegalArgumentException( "no streams provided" );
        }

        while ( isRunning && ! Thread.currentThread().isInterrupted() ) {
            for (IStream iStream : streams) {
                onStreamStart( iJob, iStream );

                try {
                    streamProcessor.process( iJob, iStream );
                    onStreamEnd( iJob, iStream );
                } catch (Exception e) {
                    onStreamFail( iJob, iStream, e );
                }
            }
        }

    }

    protected void onStreamStart( IJob iJob, IStream iStream ) throws SQLException {

    }

    protected void onStreamEnd( IJob iJob, IStream iStream ) {

    }

    protected void onStreamFail( IJob iJob, IStream iStream, Exception e ) {
        // TODO : error handling
        LOGGER.error( e.getLocalizedMessage(), e );
    }


}
