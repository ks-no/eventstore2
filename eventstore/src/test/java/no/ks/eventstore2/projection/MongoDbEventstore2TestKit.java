package no.ks.eventstore2.projection;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoIterable;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.function.Consumer;

public class MongoDbEventstore2TestKit extends EventstoreEventstore2TestKit {

    protected MongoClient mongoClient;
    private static MongodExecutable mongodExecutable = null;
    private static MongodProcess mongod = null;
    private static IMongodConfig mongodConfig;
    static MongodStarter runtime;

    @BeforeAll
    public static void setUpMongoDb() throws Exception {
        Command command = Command.MongoD;

        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(command)
                .artifactStore(new ExtractedArtifactStoreBuilder()
                        .defaults(command)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(command)
                                .downloadPath("http://jenkins.usrv.ubergenkom.no/apps/")))
                .build();
        runtime = MongodStarter.getInstance(runtimeConfig);

        mongodConfig = new MongodConfigBuilder().version(Version.Main.V3_2).cmdOptions(new MongoCmdOptionsBuilder()
                .useNoPrealloc(false)
                .useSmallFiles(true)
                .useNoJournal(false)
                .enableTextSearch(true)
                .build()).build();
        mongodExecutable = runtime.prepare(mongodConfig);
        mongod = mongodExecutable.start();
    }

    @AfterAll
    public static void tearDownMongoDb() throws Exception {
        mongod.stop();
        mongodExecutable.stop();
    }

    @BeforeEach
    public void setUp() throws Exception {
        mongoClient = new MongoClient(new ServerAddress(mongodConfig.net().getServerAddress(), mongodConfig.net().getPort()));
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        final MongoIterable<String> strings = mongoClient.listDatabaseNames();
        strings.forEach((Consumer<? super String>) (db) -> mongoClient.getDatabase(db).drop());
        mongoClient.close();
    }

}
