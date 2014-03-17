package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProjectionPolymorphismEventTest extends Eventstore2TestKit {

    @Test
    public void test_that_most_spesific_method_is_called() throws Exception {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());

        testActor.tell(new TestEvent(), super.testActor());
        assertFalse(((TestProjection) testActor.underlyingActor()).eventRecieved);
        assertTrue(((TestProjection) testActor.underlyingActor()).testEventRecieved);
    }

    @Test
    public void test_that_less_specifc_handler_is_called_if_specific_handler_is_not_present() throws Exception {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());

        testActor.tell(new Test2Event(), super.testActor());
        assertTrue(((TestProjection) testActor.underlyingActor()).eventRecieved);
        assertFalse(((TestProjection) testActor.underlyingActor()).testEventRecieved);
    }

    @Test
    public void test_that_the_most_specific_handler_in_the_inheritance_hirarchy_is_called() throws Exception {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor()), UUID.randomUUID().toString());

        testActor.tell(new TestSubSubEvent(), super.testActor());
        assertFalse(((TestProjection) testActor.underlyingActor()).eventRecieved);
        assertFalse(((TestProjection) testActor.underlyingActor()).testEventRecieved);
        assertTrue(((TestProjection) testActor.underlyingActor()).subEventRecieved);
    }

    @Subscriber("TestAggregate")
    private static class TestProjection extends Projection {


        public boolean testEventRecieved = false;
        public boolean eventRecieved = false;
        public boolean subEventRecieved = false;

        public TestProjection(ActorRef eventStore) {
            super(eventStore);
        }

        @Handler
        public void handleEvent(TestEvent event){
            testEventRecieved = true;
        }

        @Handler
        public void handleEvent(Event event){
            eventRecieved = true;
        }

        @Handler
        public void handleEvent(TestSubEvent event){
            subEventRecieved = true;
        }
    }

    private static class TestEvent extends Event {
        TestEvent() {
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }
    }

    private static class Test2Event extends Event {
        Test2Event() {
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }
    }

    private static class TestSubEvent extends Event {
        TestSubEvent() {
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }
    }

    private static class TestSubSubEvent extends TestSubEvent {
        TestSubSubEvent() {
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}






