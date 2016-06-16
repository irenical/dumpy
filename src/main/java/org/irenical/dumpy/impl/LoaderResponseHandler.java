package org.irenical.dumpy.impl;

import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LoaderResponseHandler< TYPE > implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger( LoaderResponseHandler.class );

    private final DumpyDB dumpyDB;

    private final IJob iJob;

    private final IStream< TYPE > iStream;

    private final Future< ILoader.Status > loaderTask;

    private final List< ? extends IExtractor.Entity< TYPE > > loaderEntities;

    public LoaderResponseHandler(DumpyDB dumpyDB, IJob iJob, IStream< TYPE > iStream, Future< ILoader.Status > loaderTask,
                                 List< ? extends IExtractor.Entity< TYPE > > loaderEntities ) {
        this.dumpyDB = dumpyDB;
        this.iJob = iJob;
        this.iStream = iStream;
        this.loaderTask = loaderTask;
        this.loaderEntities = loaderEntities;
    }

    @Override
    public void run() {
        try {
            ILoader.Status taskStatus = loaderTask.get();

            ZonedDateTime now = ZonedDateTime.now( ZoneId.of( "Europe/Lisbon" ) );
            ZonedDateTime lastErrorStamp = ILoader.Status.ERROR.equals(taskStatus) ? now : null;

            Object[] entities = loaderEntities.stream()
                    .map(IExtractor.Entity::getId)
                    .toArray();

            dumpyDB.upsertEntities( iJob.getCode(), iStream.getCode(), entities, lastErrorStamp, now );
        } catch ( ExecutionException | InterruptedException | SQLException e ) {
            LOGGER.error( e.getLocalizedMessage(), e );
        }
    }

}
