package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.google.protobuf.Message;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.ConfigFactory;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.EventWrapper;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import no.ks.eventstore2.response.Success;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class EventStoreProtoTest extends MongoDbEventstore2TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private MongoDBJournalV2 mongodbJournal;

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private MongoDatabase journal;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        journal = mongoClient.getDatabase("Journal");
        mongodbJournal = new MongoDBJournalV2(journal, kryoClassRegistration, Arrays.asList(new String[]{"agg"}),10, null);
        mongodbJournal.open();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testSendingIncompleteEvent() throws Exception {
        for(int i = 0; i<11;i++)
            mongodbJournal.saveEvent(new EventWrapper<Message>("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").build(), super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(EventWrapper.class);
        expectMsgClass(Messages.IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").setFromJournalId(9).build(),super.testActor());
        expectMsgClass(EventWrapper.class);
    }

    @Test
    public void testLiveSubscription() throws Exception {
        for(int i = 0; i<11;i++)
            mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(new LiveSubscription("agg"),super.testActor());
        expectMsgClass(CompleteSubscriptionRegistered.class);
        actorTestActorRef.tell(new AggEvent("agg"), super.testActor());
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

    @Test
    public void testActorRestarting() throws Exception {
        mongodbJournal.saveEvent(new AggEvent("agg"));

        TestActorRef<Actor> eventstore = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        eventstore.tell(new Subscription("agg",null),super.testActor());
        expectMsgClass(AggEvent.class);
        expectMsgClass(CompleteSubscriptionRegistered.class);
        eventstore.tell(new RemoveSubscription("agg"),super.testActor());
        expectMsgClass(SubscriptionRemoved.class);

        eventstore.tell(new AggEvent("agg"),super.testActor());
        eventstore.tell(new AcknowledgePreviousEventsProcessed(),super.testActor());
        expectMsgClass(Success.class);
    }

}
