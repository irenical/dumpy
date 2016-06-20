package org.irenical.dumpy.impl;

import org.irenical.dumpy.impl.db.DumpyDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IStream;

import java.sql.SQLException;

public class LatestJobProcessor extends BaseJobProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger( LatestJobProcessor.class );

    public LatestJobProcessor(DumpyDB dumpyDB ) {
        super( dumpyDB, new LatestStreamProcessor( dumpyDB ) );
    }

    @Override
    protected void onStreamStart( IJob iJob, IStream iStream ) throws SQLException {
        Integer streamId = dumpyDB.getStreamId(iJob.getCode(), iStream.getCode());
        if (streamId == null) {
            dumpyDB.newStream(iJob.getCode(), iStream.getCode());
        }
    }

    @Override
    protected void onStreamEnd( IJob iJob, IStream iStream ) {

    }

    @Override
    protected void onStreamFail( IJob iJob, IStream iStream, Exception e ) {
        // TODO : error handling
        LOGGER.error( e.getLocalizedMessage(), e );
    }

}
