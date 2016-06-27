package org.irenical.dumpy;

import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.IJob;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;
import org.irenical.dumpy.impl.model.StreamImpl;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

import java.util.Arrays;
import java.util.List;

public class PoopsyTest {

    public static void main( String ... args ) throws Exception {
//        startup local db
        PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
        PostgresConfig postgresConfig = new PostgresConfig( Version.Main.PRODUCTION,
                new AbstractPostgresConfig.Net( "localhost", 51000 ), new AbstractPostgresConfig.Storage( "dumpy" ),
                new AbstractPostgresConfig.Timeout(),
                new AbstractPostgresConfig.Credentials( "dumpy", "dumpy" ));
        PostgresExecutable exec = runtime.prepare(postgresConfig);
        PostgresProcess postgresProcess = exec.start();

//        run whatevers
        run();

//        stop local db
        postgresProcess.stop();
    }

    private static void run() {
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

        new Dumpy().accept( iJob );
    }

}
