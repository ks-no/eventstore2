package no.ks.eventstore2.projection;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.eventstore.AsyncSubscription;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.eventstore.RemoveSubscription;
import no.ks.eventstore2.eventstore.SubscriptionRemoved;
import no.ks.eventstore2.testapplication.AggregateType;
import no.ks.eventstore2.testapplication.TestEvent;
import no.ks.eventstore2.testapplication.TestProjection;
import no.ks.eventstore2.testkit.EventStoreTestKit;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RestartProjectionTest extends EventStoreTestKit {

    private TestActorRef<TestProjection> projection;

    @BeforeEach
    public void before() {
        projection = createProjectionRef(Props.create(TestProjection.class, testActor()));
    }

    @Test
    public void test_restart() throws Exception {
        expectMsgClass(AsyncSubscription.class);
        projection.tell(createEvent(0), super.testActor());
        projection.tell(createEvent(1), super.testActor());
        projection.tell("restart", super.testActor());
        projection.tell(createEvent(2), super.testActor());
        projection.tell(createEvent(3), super.testActor());
        expectMsgClass(RemoveSubscription.class);
        projection.tell(createEvent(4), super.testActor());
        projection.tell(new SubscriptionRemoved(AggregateType.TEST_AGGREGATE), super.testActor());
        expectMsgClass(AsyncSubscription.class);
        projection.tell(createEvent(0), super.testActor());
        projection.tell(createEvent(1), super.testActor());
        projection.tell(createEvent(2), super.testActor());
        projection.tell(createEvent(3), super.testActor());
        projection.tell(new CompleteSubscriptionRegistered(AggregateType.TEST_AGGREGATE),super.testActor());
        final List<TestEvent> events = (List<TestEvent>)Await.result(ask(projection, call("getEvents"), 3000), Duration.create(3, TimeUnit.SECONDS));
        assertEquals(4, events.size());
    }

    private TestEvent createEvent(long jid) {
        final TestEvent event = new TestEvent();
        event.setCreated(DateTime.now());
        event.setJournalid(String.valueOf(jid));
        return event;
    }


}
