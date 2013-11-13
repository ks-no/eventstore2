package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.projection.Aggregate;
import no.ks.eventstore2.projection.EventHandler;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class SagaWithNewAnnotationTest extends Eventstore2TestKit{


    @Test
    public void test_that_saga_with_new_annotaions_handle_correct_event() throws Exception {
        Props props = Props.create(SagaWithNewAnotation.class, "a test id", super.testActor(), new SagaInMemoryRepository());
        TestActorRef<SagaWithNewAnotation> testActor = TestActorRef.create(_system, props, UUID.randomUUID().toString());

        testActor.tell(new TestEvent("a test id"), super.testActor());
        assertEquals(Saga.STATE_FINISHED, testActor.underlyingActor().getState());
    }

    @SagaEventIdProperty("testId")
    @Aggregate("TestAggregate")
    private static class SagaWithNewAnotation extends Saga {

        public SagaWithNewAnotation(String id, ActorRef commandDispatcher, SagaRepository repository) {
            super(id, commandDispatcher, repository);
        }

        @EventHandler
        public void handleEvent(TestEvent event){
            transitionState(STATE_FINISHED);
        }
    }

    private static class TestEvent extends Event{
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
