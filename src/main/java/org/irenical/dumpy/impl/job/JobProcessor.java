package org.irenical.dumpy.impl.job;

import org.irenical.dumpy.impl.stream.StreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IStream;

import java.sql.SQLException;

public class JobProcessor extends BaseJobProcessor {

    public JobProcessor(DumpyDB dumpyDB ) {
        super( dumpyDB, new StreamProcessor( dumpyDB ) );
    }

    @Override
    protected void onStreamStart( IJob iJob, IStream iStream ) throws SQLException {
        Integer streamId = dumpyDB.getStreamId(iJob.getCode(), iStream.getCode());
        if (streamId == null) {
            dumpyDB.newStream(iJob.getCode(), iStream.getCode());
        }
    }

}
