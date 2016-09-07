package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.Inbox;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.mongodb.client.MongoDatabase;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class EvenstoreTestReadAggregateProtoEvents extends MongoDbEventstore2TestKit{

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private MongoDBJournalV2 journal;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MongoDatabase db = mongoClient.getDatabase("Journal");
        journal = new MongoDBJournalV2(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg", "agg2"}), 10, null);
    }

    @Test
    public void testReadEventsForOneAggregateId() throws Exception {
        for(int i = 0; i<3;i++)
            journal.saveEvent(ProtobufHelper.newEventWrapper("agg", "id", -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()));
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(journal));
        Inbox inbox = Inbox.create(_system);
        actorTestActorRef.tell(Messages.RetreiveAggregateEvents.newBuilder().setAggregateType("agg").setAggregateRootId("id").build(), inbox.getRef());
        final Object receive = inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertEquals(receive.getClass(), Messages.EventWrapperBatch.class);
        assertEquals("id", ((Messages.EventWrapperBatch)receive).getAggregateRootId());
        assertEquals(3, ((Messages.EventWrapperBatch) receive).getEventsCount());
    }

    @Test
    public void testReadEventsForOneAggregateIdAndContinueWhenBatchIsFull() throws Exception {
        for(int i = 0; i<11;i++)
            journal.saveEvent(ProtobufHelper.newEventWrapper("agg2", "id", -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()));
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(journal));
        Inbox inbox = Inbox.create(_system);
        actorTestActorRef.tell(Messages.RetreiveAggregateEvents.newBuilder().setAggregateType("agg2").setAggregateRootId("id").build(), inbox.getRef());
        Messages.EventWrapperBatch receive = (Messages.EventWrapperBatch) inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertEquals("id",receive.getAggregateRootId());
        assertEquals(10,receive.getEventsCount());
        actorTestActorRef.tell(Messages.RetreiveAggregateEvents.newBuilder()
        .setAggregateType("agg2")
                .setAggregateRootId("id")
                .setFromJournalId(10).build(), inbox.getRef());
        receive = (Messages.EventWrapperBatch) inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertEquals("agg2", receive.getAggregateType());
        assertEquals(1, receive.getEventsCount());
    }
}
