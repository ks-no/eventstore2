package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.google.protobuf.Any;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.ConfigFactory;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import no.ks.eventstore2.response.Success;
import org.joda.time.DateTime;
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
            mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").build(), super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(Messages.EventWrapper.class);
        expectMsgClass(Messages.IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").setFromJournalId(9).build(),super.testActor());
        expectMsgClass(Messages.EventWrapper.class);
    }

    @Test
    public void testLiveSubscription() throws Exception {
        for(int i = 0; i<11;i++)
            mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.LiveSubscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
        actorTestActorRef.tell(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()), super.testActor());
        expectMsgClass(Messages.EventWrapper.class);
    }

    @Test
    public void testNoImcompleteIfSentAllOnFristSubscribe() throws Exception {
        for(int i = 0; i<3;i++)
            mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        for(int i = 0; i<3;i++)
            expectMsgClass(Messages.EventWrapper.class);
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testNoIncompleteIf1000Events() throws Exception {
        for(int i = 0; i<10;i++)
            mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(Messages.EventWrapper.class);
        expectMsgClass(Messages.IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").setFromJournalId(10).build(),super.testActor());
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testAsyncSubscription() throws Exception {
        for(int i = 0; i<10;i++)
            mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.AsyncSubscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        for(int i = 0; i<10;i++)
            expectMsgClass(Messages.EventWrapper.class);
        expectMsgClass(Messages.IncompleteSubscriptionPleaseSendNew.class);
        actorTestActorRef.tell(Messages.AsyncSubscription.newBuilder().setAggregateType("agg").setFromJournalId(10).build(),super.testActor());
        expectMsgClass(Messages.CompleteAsyncSubscriptionPleaseSendSyncSubscription.class);
        actorTestActorRef.tell(Messages.Subscription.newBuilder().setAggregateType("agg").setFromJournalId(10).build(),super.testActor());
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
    }

    @Test
    public void testActorRestarting() throws Exception {
        mongodbJournal.saveEvent(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()));

        TestActorRef<Actor> eventstore = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        eventstore.tell(Messages.Subscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        expectMsgClass(Messages.EventWrapper.class);
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
        eventstore.tell(Messages.RemoveSubscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        expectMsgClass(Messages.SubscriptionRemoved.class);

        eventstore.tell(ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build()),super.testActor());
        eventstore.tell(Messages.AcknowledgePreviousEventsProcessed.getDefaultInstance(),super.testActor());
        expectMsgClass(Messages.Success.class);
    }

    @Test
    public void testSavingEventPublishedEventWithJournalid() throws Exception {
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.LiveSubscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);
        Messages.EventWrapper agg = ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build());
        actorTestActorRef.tell(agg, super.testActor());

        expectMsg(ProtobufHelper.newEventWrapper("agg", "1", 0, 1, new DateTime(agg.getOccurredOn()), Order.SearchRequest.newBuilder().build()));
        agg = ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build());
        actorTestActorRef.tell(agg, super.testActor());
        expectMsg(ProtobufHelper.newEventWrapper("agg", "1", 1, 2, new DateTime(agg.getOccurredOn()), Order.SearchRequest.newBuilder().build()));
    }

    @Test
    public void testSavingEventsPublishedEventsWithJournalid() throws Exception {
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(mongodbJournal));
        actorTestActorRef.tell(Messages.LiveSubscription.newBuilder().setAggregateType("agg").build(),super.testActor());
        expectMsgClass(Messages.CompleteSubscriptionRegistered.class);

        Messages.EventWrapper agg = ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build());
        Messages.EventWrapper agg2 = ProtobufHelper.newEventWrapper("agg", "1", -1, Order.SearchRequest.newBuilder().build());
        actorTestActorRef.tell(Messages.EventWrapperBatch.newBuilder().setAggregateType("agg").setAggregateRootId("1").addEvents(agg).addEvents(agg2).build(), super.testActor());

        expectMsg(ProtobufHelper.newEventWrapper("agg", "1", 0, 1, new DateTime(agg.getOccurredOn()), Order.SearchRequest.newBuilder().build()));

        actorTestActorRef.tell(agg, super.testActor());
        expectMsg(ProtobufHelper.newEventWrapper("agg", "1", 1, 2, new DateTime(agg2.getOccurredOn()), Order.SearchRequest.newBuilder().build()));
    }
}
