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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LoaderResponseHandler< TYPE, ERROR extends Exception > implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger( LoaderResponseHandler.class );

    private final DumpyDB dumpyDB;

    private final IJob iJob;

    private final IStream< TYPE, ERROR > iStream;

    private final Future< Map< ? extends IExtractor.Entity< TYPE >, ILoader.Status> > loaderTask;


    public LoaderResponseHandler(DumpyDB dumpyDB, IJob iJob, IStream< TYPE, ERROR > iStream,
                                 Future<Map< ? extends IExtractor.Entity< TYPE >, ILoader.Status>> loaderTask ) {
        this.dumpyDB = dumpyDB;
        this.iJob = iJob;
        this.iStream = iStream;
        this.loaderTask = loaderTask;
    }

    @Override
    public void run() {
        try {
            Map< ? extends IExtractor.Entity< TYPE >, ILoader.Status> taskStatus = loaderTask.get();

//            split result into successes and errors so we upsert them accordingly
            List< IExtractor.Entity<TYPE> > success = new LinkedList<>();
            List< IExtractor.Entity<TYPE> > errors = new LinkedList<>();
            for (IExtractor.Entity< TYPE > entity : taskStatus.keySet()) {
                ILoader.Status status = taskStatus.get(entity);
                if (ILoader.Status.SUCCESS.equals( status ) ) {
                    success.add( entity );
                } else {
                    errors.add( entity );
                }
            }

            LOGGER.info( "[ loaderHandler( " + Thread.currentThread().getName() + " ) ] " +
                    "success=" + success.size() + "; error=" + errors.size() );

            ZonedDateTime now = ZonedDateTime.now( ZoneId.of( "Europe/Lisbon" ) );

//            maybe this should be optimized into a single datastructure + single query (one upsert)
//            upsert success entities (no error stamp)
            dumpyDB.upsertEntities( iJob.getCode(), iStream.getCode(), convert( success ), null, now );
//            upsert error entities
            dumpyDB.upsertEntities( iJob.getCode(), iStream.getCode(), convert( errors ), now, now );

        } catch ( ExecutionException | InterruptedException | SQLException e ) {
            LOGGER.error( e.getLocalizedMessage(), e );
        }
    }

    private Object[] convert( List< IExtractor.Entity< TYPE > > entities ) {
        return entities.stream()
                .map(IExtractor.Entity::getId)
                .toArray();
    }

}
