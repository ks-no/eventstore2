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

public class SagaNewAnnotationStyleTest extends Eventstore2TestKit{


    @Test
    public void test_that_methods_with_eventHandler_annotations_are_called_when_saga_receives_events() throws Exception {
        Props props = Props.create(SagaWithNewAnotation.class, "a test id", super.testActor(), new SagaInMemoryRepository());
        TestActorRef<SagaWithNewAnotation> testActor = TestActorRef.create(_system, props, UUID.randomUUID().toString());

        testActor.tell(new TestEvent("a test id"), super.testActor());
        assertEquals(Saga.STATE_FINISHED, testActor.underlyingActor().getState());
    }

    @SagaEventIdProperty("testId")
    @Subscriber("TestAggregate")
     static class SagaWithNewAnotation extends Saga {

        public SagaWithNewAnotation(String id, ActorRef commandDispatcher, SagaRepository repository) {
            super(id, commandDispatcher, repository);
        }

        @Handler
        public void handleEvent(TestEvent event){
            transitionState(STATE_FINISHED);
        }
    }

     static class TestEvent extends Event{
        private String testId;

        private TestEvent(String testId) {
            this.testId = testId;
        }

        @Override
        public String getLogMessage() {
            return null;
        }

        public String getTestId(){
            return testId;
        }

    }
}
