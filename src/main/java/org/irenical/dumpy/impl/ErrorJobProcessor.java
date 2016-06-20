package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.impl.db.DumpyDB;

import java.sql.SQLException;

public class ErrorJobProcessor extends BaseJobProcessor {


    public ErrorJobProcessor( DumpyDB dumpyDB ) {
        super( dumpyDB, new ErrorStreamProcessor( dumpyDB ) );
    }

    @Override
    protected void onStreamStart(IJob iJob, IStream iStream) throws SQLException {
        super.onStreamStart(iJob, iStream);
    }

    @Override
    protected void onStreamEnd(IJob iJob, IStream iStream) {
        super.onStreamEnd(iJob, iStream);
    }

    @Override
    protected void onStreamFail(IJob iJob, IStream iStream, Exception e) {
        super.onStreamFail(iJob, iStream, e);
    }

}
