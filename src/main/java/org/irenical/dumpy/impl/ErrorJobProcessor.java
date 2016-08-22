package org.irenical.dumpy.impl;

import org.irenical.dumpy.impl.db.DumpyDB;

public class ErrorJobProcessor extends BaseJobProcessor {

    public ErrorJobProcessor( DumpyDB dumpyDB ) {
        super( dumpyDB, new ErrorStreamProcessor( dumpyDB ) );
    }

}
