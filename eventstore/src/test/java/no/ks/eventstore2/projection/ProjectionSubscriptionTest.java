package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.eventstore.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ProjectionSubscriptionTest extends MongoDbEventstore2TestKit{

    @Test
    public void test_that_a_projection_can_save_and_load_snapshot() throws Exception {

        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());
        expectMsgClass(AsyncSubscription.class);
        testActor.tell(new TestEvent(),super.testActor());
        testActor.tell(new IncompleteSubscriptionPleaseSendNew("TestAggregate"),super.testActor());
        expectMsgClass(AsyncSubscription.class);
        testActor.tell(new CompleteAsyncSubscriptionPleaseSendSyncSubscription("TestAggregate"),super.testActor());
        expectMsgClass(Subscription.class);
        testActor.tell(new CompleteSubscriptionRegistered("TestAggregate"),super.testActor());
    }



    @Subscriber("TestAggregate")
    private static class TestProjection extends Projection {

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
}
