package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.mongodb.client.MongoDatabase;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EvenstoreTestReadAggregateEvents extends MongoDbEventstore2TestKit{

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
        journal = new MongoDBJournalV2(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg", "agg2"}), 10);
    }

    @Test
    public void testReadEventsForOneAggregateId() throws Exception {
        for(int i = 0; i<3;i++)
            journal.saveEvent(new AggEvent("id","agg"));
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(journal));
        actorTestActorRef.tell(new RetreiveAggregateEvents("agg", "id", null), super.testActor());
        List<Event> events = new ArrayList<>();
        for(int i = 0; i<3;i++)
            events.add(new AggEvent("id","agg"));
        expectMsg(new EventBatch("agg", "id", events, true));
    }

    @Test
    public void testReadEventsForOneAggregateIdAndContinueWhenBatchIsFull() throws Exception {
        for(int i = 0; i<11;i++)
            journal.saveEvent(new AggEvent("id","agg2"));
        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(journal));
        actorTestActorRef.tell(new RetreiveAggregateEvents("agg2", "id", null), super.testActor());
        List<Event> events = new ArrayList<>();
        for(int i = 0; i<10;i++)
            events.add(new AggEvent("id","agg2"));
        expectMsg(new EventBatch("agg2", "id", events, false));
        actorTestActorRef.tell(new RetreiveAggregateEvents("agg2", "id", "10"), super.testActor());
        events.clear();
        for(int i = 0; i<1;i++)
            events.add(new AggEvent("id","agg2"));
        expectMsg(new EventBatch("agg2", "id", events, true));
    }
}
