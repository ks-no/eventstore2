package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.esotericsoftware.kryo.Kryo;
import com.mongodb.DB;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class EventStoreTest extends MongoDbEventstore2TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private MongoDBJournalV2 mongodbJournal;

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private DB journal;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        journal = mongoClient.getDB("Journal");
        mongodbJournal = new MongoDBJournalV2(journal, kryoClassRegistration, Arrays.asList(new String[]{"agg"}),10);
        mongodbJournal.open();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testSendingIncompleteEvent() throws Exception {
        for(int i = 0; i<11;i++)
            mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(new Subscription("agg","9"),super.testActor());
        expectMsgClass(AggEvent.class);
    }

    @Test
    public void testNoImcompleteIfSentAllOnFristSubscribe() throws Exception {
        for(int i = 0; i<3;i++)
            mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<3;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testNoIncompleteIf1000Events() throws Exception {
        for(int i = 0; i<10;i++)
            mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(new Subscription("agg",null),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(new Subscription("agg","10"),super.testActor());
        expectMsgClass(CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testAsyncSubscription() throws Exception {
        for(int i = 0; i<10;i++)
            mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(new AsyncSubscription("agg",null),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(AggEvent.class);
        expectMsgClass(IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(new AsyncSubscription("agg","10"),super.testActor());
        expectMsgClass(CompleteAsyncSubscriptionPleaseSendSyncSubscription.class);
        actorTestActorRef.tell(new Subscription("agg","10"),super.testActor());
        expectMsgClass(CompleteSubscriptionRegistered.class);
    }

}
