package no.ks.eventstore2.projection;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
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
    static MongodStarter runtime = MongodStarter.getDefaultInstance();

    @BeforeClass
    public static void setUpMongoDb() throws Exception {
        mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION).build();
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
