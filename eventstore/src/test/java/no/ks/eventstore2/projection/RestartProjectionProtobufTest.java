package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import no.ks.eventstore2.util.IdUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RestartProjectionProtobufTest extends EventstoreEventstore2TestKit {

    private TestActorRef<TestProjection> projection;

    @BeforeEach
    public void before() {

        TestActorRef<TestProjection> actorRef = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), IdUtil.createUUID());

        projection = actorRef;
    }

    @Test
    public void test_restart() throws Exception {
        fail();
//        expectMsgClass(Messages.AsyncSubscription.class);
//        projection.tell(createEvent(0), super.testActor());
//        projection.tell(createEvent(1), super.testActor());
//        projection.tell("restart", super.testActor());
//        projection.tell(createEvent(2), super.testActor());
//        projection.tell(createEvent(3), super.testActor());
//        expectMsgClass(Messages.RemoveSubscription.class);
//        projection.tell(createEvent(4), super.testActor());
//        projection.tell(Messages.SubscriptionRemoved.newBuilder().setAggregateType("Test").build(), super.testActor());
//        expectMsgClass(Messages.AsyncSubscription.class);
//        projection.tell(createEvent(0), super.testActor());
//        projection.tell(createEvent(1), super.testActor());
//        projection.tell(createEvent(2), super.testActor());
//        projection.tell(createEvent(3), super.testActor());
//        projection.tell(Messages.CompleteSubscriptionRegistered.newBuilder().setAggregateType("Test").build(),super.testActor());
//        final List<TestEvent> events = (List<TestEvent>)Await.result(ask(projection, call("getEvents"), 3000), Duration.create(3, TimeUnit.SECONDS));
//        assertEquals(4, events.size());
    }

    private Messages.EventWrapper createEvent(long jid) {
        return ProtobufHelper.newEventWrapper("TestAggregate", "id", jid, Order.SearchResult.getDefaultInstance()).toBuilder().setVersion(jid).build();
    }
    @Subscriber("TestAggregate")
    private static class TestProjection extends ProjectionProtobuf {

        public boolean testEventRecieved = false;
        private Messages.EventWrapper data = null;
        List<Messages.EventWrapper> events = new ArrayList<>();

        public TestProjection(ActorRef eventStore) {
            super(eventStore);
        }


        @Handler
        public void handleEvent(Order.SearchResult event){
            testEventRecieved = true;
            data = currentMessage();
            events.add(currentMessage());
        }

        public List<Messages.EventWrapper> getEvents(){
            return events;
        }

    }

}
