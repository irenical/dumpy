package org.irenical.dumpy;

import org.irenical.booty.Booty;
import org.irenical.booty.BootyConfig;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.impl.ErrorJobProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.LatestJobProcessor;
import org.irenical.loggy.Loggy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Dumpy implements Consumer< IJob > {

    private static final Logger LOGGER = LoggerFactory.getLogger( Dumpy.class );

    private final ExecutorService executorService = Executors.newFixedThreadPool( 2 );

    public Dumpy() {

    }

    @Override
    public void accept(IJob iJob) {
        DumpyDB dumpyDB = new DumpyDB();

        IJobProcessor latestJobProcessor = new LatestJobProcessor( dumpyDB );
        IJobProcessor errorJobProcessor = new ErrorJobProcessor( dumpyDB );

        BootyConfig config = new BootyConfig();
        config.setLifecycleSupplier( () -> Arrays.asList( new Loggy()
                , dumpyDB
                , latestJobProcessor
                , errorJobProcessor
        ) );

        Booty.build( config ).start();

        executorService.execute(() -> latestJobProcessor.accept( iJob ));
        executorService.execute(() -> errorJobProcessor.accept( iJob ));

        try {
            boolean awaitTermination = false;
            while ( ! awaitTermination ) {
                LOGGER.debug( "[ dumpy ] waiting for jobs to finish" );
                awaitTermination = executorService.awaitTermination( 10, TimeUnit.SECONDS );
            }
        } catch (InterruptedException e) {
            LOGGER.error( e.getLocalizedMessage(), e );
        }
    }

}
