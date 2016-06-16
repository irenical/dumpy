package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IStreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.api.IStream;

import java.sql.SQLException;
import java.util.List;

public class JobProcessor implements IJobProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( JobProcessor.class );

    private final IStreamProcessor streamProcessor;

    private final DumpyDB dumpyDB;

    private boolean isRunning = false;

    public JobProcessor( DumpyDB dumpyDB, IStreamProcessor streamProcessor ) {
        this.dumpyDB = dumpyDB;
        this.streamProcessor = streamProcessor;
    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        isRunning = true;
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        isRunning = false;
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

    private void onStreamStart( IJob iJob, IStream iStream ) throws SQLException {
        Integer streamId = dumpyDB.getStreamId(iJob.getCode(), iStream.getCode());
        if (streamId == null) {
            dumpyDB.newStream(iJob.getCode(), iStream.getCode());
        }
    }

    private void onStreamEnd( IJob iJob, IStream iStream ) {

    }

    private void onStreamFail( IJob iJob, IStream iStream, Exception e ) {
        // TODO : error handling
        LOGGER.error( e.getLocalizedMessage(), e );
    }

}
