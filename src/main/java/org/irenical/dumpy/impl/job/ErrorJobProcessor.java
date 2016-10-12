package org.irenical.dumpy.impl.job;

import org.irenical.dumpy.impl.stream.ErrorStreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;

public class ErrorJobProcessor extends BaseJobProcessor {

    public ErrorJobProcessor( DumpyDB dumpyDB ) {
        super( dumpyDB, new ErrorStreamProcessor( dumpyDB ) );
    }

}
