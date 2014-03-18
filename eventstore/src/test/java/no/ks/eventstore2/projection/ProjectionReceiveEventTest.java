package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.eventstore.Subscription;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class ProjectionReceiveEventTest extends Eventstore2TestKit {

    @Test
    public void test_that_a_projection_handles_events_using_annotated_methods() throws Exception {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());

        testActor.tell(new TestEvent(), super.testActor());
        assertTrue(((TestProjection) testActor.underlyingActor()).testEventRecieved);
    }

    @Test
    public void test_that_a_projection_subscribes_to_the_aggregates_of_its_event_handlers() throws Exception {
        TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());
        expectMsg(new Subscription("TestAggregate"));
    }


    @Subscriber("TestAggregate")
    private static class TestProjection extends Projection {


        public boolean testEventRecieved = false;

        public TestProjection(ActorRef eventStore) {
            super(eventStore);
        }

        @Handler
        public void handleEvent(TestEvent event){
            testEventRecieved = true;
        }
    }

    private static class TestEvent extends Event {
		private static final long serialVersionUID = 1L;

		@Override
        public String getLogMessage() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
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






