package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.esotericsoftware.kryo.Kryo;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class EventStoreTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private LevelDbJournalStorage levelDbJournalStorage;

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };

    public EventStoreTest() {
        super(_system);
    }

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("target/journal"));

        levelDbJournalStorage = new LevelDbJournalStorage("target/journal", kryoClassRegistration,10L);
        levelDbJournalStorage.open();

    }

    @After
    public void tearDown() throws Exception {
        levelDbJournalStorage.close();
        FileUtils.deleteDirectory(new File("target/journal"));
    }

    @Test
    public void testSendingIncompleteEvent() throws Exception {
        for(int i = 0; i<11;i++)
            levelDbJournalStorage.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(levelDbJournalStorage));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(new Subscription("agg","0000000000000000009"),super.testActor());
        expectMsgClass(AggEvent.class);
    }

    @Test
    public void testNoImcompleteIfSentAllOnFristSubscribe() throws Exception {
        for(int i = 0; i<3;i++)
            levelDbJournalStorage.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(levelDbJournalStorage));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<3;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testNoIncompleteIf1000Events() throws Exception {
        for(int i = 0; i<10;i++)
            levelDbJournalStorage.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(levelDbJournalStorage));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(new Subscription("agg","0000000000000000009"),super.testActor());
        expectMsgClass(CompleteSubscriptionRegistered.class);
    }

}
