package no.ks.eventstore2.projection;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import no.ks.eventstore2.Eventstore2TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class MongoDbEventstore2TestKit extends Eventstore2TestKit {

    protected MongoClient mongoClient;
    private static MongodExecutable mongodExecutable = null;
    private static MongodProcess mongod = null;
    private static IMongodConfig mongodConfig;
    static MongodStarter runtime;

    @BeforeClass
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

        mongodConfig = new MongodConfigBuilder().version(Version.Main.V3_0).cmdOptions(new MongoCmdOptionsBuilder()
                .useNoPrealloc(false)
                .useSmallFiles(true)
                .useNoJournal(false)
                .enableTextSearch(true)
                .build()).build();
        mongodExecutable = runtime.prepare(mongodConfig);
        mongod = mongodExecutable.start();
    }

    @AfterClass
    public static void tearDownMongoDb() throws Exception {
        mongod.stop();
        mongodExecutable.stop();
    }

    @Before
    public void setUp() throws Exception {
        mongoClient = new MongoClient(new ServerAddress(mongodConfig.net().getServerAddress(), mongodConfig.net().getPort()));
    }

    @After
    public void tearDown() throws Exception {
        for (String dbname : mongoClient.getDatabaseNames()) {
            mongoClient.dropDatabase(dbname);
        }
        mongoClient.close();
    }

}
