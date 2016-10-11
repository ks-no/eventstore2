package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ProjectionProtobufSubscriptionTest extends MongoDbEventstore2TestKit{

    @Test
    public void test_that_a_projection_can_save_and_load_snapshot() throws Exception {

        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());
        expectMsgClass(Messages.AsyncSubscription.class);
        testActor.tell(ProtobufHelper.newEventWrapper("TestAggregate", "id", 1, Order.SearchRequest.getDefaultInstance()),super.testActor());
        testActor.tell(Messages.IncompleteSubscriptionPleaseSendNew.newBuilder().setAggregateType("TestAggregate").build(),super.testActor());
        expectMsgClass(Messages.AsyncSubscription.class);
        testActor.tell(Messages.CompleteAsyncSubscriptionPleaseSendSyncSubscription.newBuilder().setAggregateType("TestAggregate").build(),super.testActor());
        expectMsgClass(Messages.Subscription.class);
        testActor.tell(Messages.CompleteSubscriptionRegistered.newBuilder().setAggregateType("TestAggregate").build(),super.testActor());
    }

    @Subscriber("TestAggregate")
    private static class TestProjection extends ProjectionProtobuf {

        public boolean testEventRecieved = false;
        private Map<String, Event> data = new HashMap<String, Event>();

        public TestProjection(ActorRef eventStore) {
            super(eventStore);
        }

        @Handler
        public void handleEvent(TestEvent event){
            testEventRecieved = true;
            data.put("1",event);
        }

    }
    private static class TestEvent extends Event {
        private static final long serialVersionUID = 1L;

        TestEvent() {
            setJournalid("000000001");
        }

        @Override
        public String getLogMessage() {
            return null;
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }

        @Override
        public String getAggregateType() {
            return "TestAggregate";
        }
    }

    @Test
    public void name() throws Exception {
        final Order.SearchRequest defaultInstance = Order.SearchRequest.newBuilder().setQuery("Query er lang").build();

        System.out.println("PRINT:" + ProtobufHelper.toLog(ProtobufHelper.newEventWrapper("TestAggregate", "id", 1, defaultInstance)));

    }
}
