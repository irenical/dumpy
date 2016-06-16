package org.irenical.dumpy;

import org.irenical.booty.Booty;
import org.irenical.booty.BootyConfig;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.IJobProcessor;
import org.irenical.dumpy.api.IStreamProcessor;
import org.irenical.dumpy.impl.db.DumpyDB;
import org.irenical.dumpy.impl.JobProcessor;
import org.irenical.dumpy.impl.StreamProcessor;
import org.irenical.loggy.Loggy;

import java.util.Arrays;
import java.util.function.Consumer;

public class Dumpy implements Consumer< IJob > {

    public Dumpy() {

    }

    @Override
    public void accept(IJob iJob) {
        DumpyDB dumpyDB = new DumpyDB();

        IStreamProcessor streamProcessor = new StreamProcessor( dumpyDB );
        IJobProcessor jobProcessor = new JobProcessor( dumpyDB, streamProcessor );

        BootyConfig config = new BootyConfig();
        config.setLifecycleSupplier( () -> Arrays.asList( new Loggy(), dumpyDB, streamProcessor, jobProcessor ) );

        Booty.build( config ).start();

        jobProcessor.accept( iJob );
    }

}
