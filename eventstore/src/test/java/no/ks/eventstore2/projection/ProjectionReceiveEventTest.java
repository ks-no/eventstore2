package no.ks.eventstore2.projection;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.testapplication.TestEvent;
import no.ks.eventstore2.testapplication.TestProjection;
import no.ks.eventstore2.testkit.EventStoreTestKit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ProjectionReceiveEventTest extends EventStoreTestKit {

    private TestActorRef<TestProjection> projection;

    @Before
    public void before() {
        projection = createProjectionRef(Props.create(TestProjection.class, testActor()));
    }

    @Test
    public void test_that_a_projection_handles_events_using_annotated_methods() throws Exception {
        projection.tell(new TestEvent(), testActor());
        assertTrue(TestProjection.askIsEventReceived(projection));
    }

    @Test
    public void test_that_a_projection_subscribes_to_the_aggregates_of_its_event_handlers() throws Exception {
        expectMsg(new Subscription("TestAggregate"));
    }

}






