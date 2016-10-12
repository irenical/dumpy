package org.irenical.dumpy.impl.job;

import org.irenical.dumpy.impl.stream.LatestStreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IStream;

import java.sql.SQLException;

public class LatestJobProcessor extends BaseJobProcessor {

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

}
