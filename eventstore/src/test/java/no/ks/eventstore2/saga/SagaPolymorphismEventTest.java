package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Subscriber;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SagaPolymorphismEventTest extends Eventstore2TestKit{

    @Test
    public void test_that_a_event_is_handled_if_a_handler_has_the_event_as_parameter() throws Exception {
        Props props = Props.create(SagaWithNewAnotation.class, "a test id", super.testActor(), new SagaInMemoryRepository());
        TestActorRef<SagaWithNewAnotation> testActor = TestActorRef.create(_system, props, UUID.randomUUID().toString());

        testActor.tell(new TestEvent("a test id"), super.testActor());
        assertTrue(testActor.underlyingActor().a_event_handled);
        assertFalse(testActor.underlyingActor().super_event_handled);
        assertFalse(testActor.underlyingActor().sub_event_handled);
    }

    @Test
    public void test_that_the_handler_with_the_most_spesific_type_of_event_as_a_parameter_is_called() throws Exception {
        Props props = Props.create(SagaWithNewAnotation.class, "a test id", super.testActor(), new SagaInMemoryRepository());
        TestActorRef<SagaWithNewAnotation> testActor = TestActorRef.create(_system, props, UUID.randomUUID().toString());

        testActor.tell(new SubEvent("a test id"), super.testActor());
        assertFalse(testActor.underlyingActor().a_event_handled);
        assertFalse(testActor.underlyingActor().super_event_handled);
        assertTrue(testActor.underlyingActor().sub_event_handled);
    }

    @Test
    public void test_that_the_handler_with_the_most_spesific_type_of_event_as_a_parameter_is_called_if_there_is_no_handler_for_the_spesific_type() throws Exception {
        Props props = Props.create(SagaWithNewAnotation.class, "a test id", super.testActor(), new SagaInMemoryRepository());
        TestActorRef<SagaWithNewAnotation> testActor = TestActorRef.create(_system, props, UUID.randomUUID().toString());

        testActor.tell(new AnotherSubEvent("a test id"), super.testActor());
        assertFalse(testActor.underlyingActor().a_event_handled);
        assertTrue(testActor.underlyingActor().super_event_handled);
        assertFalse(testActor.underlyingActor().sub_event_handled);
    }

    @SagaEventIdProperty("testId")
    @Subscriber("TestAggregate")
     static class SagaWithNewAnotation extends Saga {

        public boolean a_event_handled = false;
        public boolean super_event_handled = false;
        public boolean sub_event_handled = false;

        public SagaWithNewAnotation(String id, ActorRef commandDispatcher, SagaRepository repository) {
            super(id, commandDispatcher, repository);
        }

        @Handler
        public void handleEvent(TestEvent event){
            a_event_handled = true;
        }

        @Handler
        public void handleEvent(SuperEvent event){
            super_event_handled = true;
        }

        @Handler
        public void handleEvent(SubEvent event){
            sub_event_handled = true;
        }
    }

    private static class TestEvent extends Event {
        private String testId;

        TestEvent(String testId) {
            this.testId = testId;
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;
        }

        public String getTestId() {
            return testId;
        }
    }

    private abstract static class SuperEvent extends Event {
        private String testId;

        SuperEvent(String testId) {
            this.testId = testId;
            setAggregateId("TestAggregate");
        }

        public String getTestId() {
            return testId;
        }
    }

    private static class SubEvent extends SuperEvent {
        SubEvent(String testId) {
            super(testId);
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;
        }
    }

    private static class AnotherSubEvent extends SuperEvent {
        AnotherSubEvent(String testId) {
            super(testId);
            setAggregateId("TestAggregate");
        }

        @Override
        public String getLogMessage() {
            return null;
        }
    }
}
