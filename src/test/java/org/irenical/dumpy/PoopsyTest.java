package org.irenical.dumpy;

import org.irenical.booty.Booty;
import org.irenical.booty.BootyConfig;
import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.impl.model.StreamImpl;
import org.irenical.lifecycle.LifeCycle;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

import java.util.Arrays;
import java.util.List;

public class PoopsyTest implements LifeCycle {

    public static class EmbeddedPostgreSQL implements LifeCycle {

        private PostgresProcess postgresProcess;

        @Override
        public void start() throws Exception {
            PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
            PostgresConfig postgresConfig = new PostgresConfig( Version.Main.PRODUCTION,
                    new AbstractPostgresConfig.Net( "localhost", 51000 ), new AbstractPostgresConfig.Storage( "dumpy" ),
                    new AbstractPostgresConfig.Timeout(),
                    new AbstractPostgresConfig.Credentials( "dumpy", "dumpy" ));
            PostgresExecutable exec = runtime.prepare(postgresConfig);
            postgresProcess = exec.start();
        }

        @Override
        public <ERROR extends Exception> void stop() throws ERROR {
            postgresProcess.stop();
        }

        @Override
        public <ERROR extends Exception> boolean isRunning() throws ERROR {
            return postgresProcess.isProcessRunning();
        }

    }


    public static void main( String ... args ) throws Exception {
        BootyConfig bootyConfig = new BootyConfig();
        bootyConfig.setOnError(e -> {
            throw new RuntimeException( e.getLocalizedMessage(), e );
        });
        bootyConfig.setLifecycleSupplier(() -> Arrays.asList( new EmbeddedPostgreSQL(), new PoopsyTest()) );
        bootyConfig.setShutdownHook( true );

        LifeCycle app = Booty.build(bootyConfig);
        app.start();
    }


    private final Dumpy dumpy = new Dumpy( false );

    public PoopsyTest() {

    }

    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        IJob iJob = new IJob() {
            @Override
            public String getCode() {
                return "Poopsy";
            }

            @Override
            public List<IStream> getStreams() {
                ILoader< Integer > iLoader = new PoopsyLoader();
                IExtractor< Integer, Exception > iExtractor = new PoopsyExtractor();

                return Arrays.asList(
                        new StreamImpl<>("1", iExtractor, iLoader),
                        new StreamImpl<>("2", iExtractor, iLoader),
                        new StreamImpl<>("3", iExtractor, iLoader)
                );
            }
        };

        dumpy.start();
        dumpy.accept( iJob );
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        dumpy.stop();
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return dumpy.isRunning();
    }

}
