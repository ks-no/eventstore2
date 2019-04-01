package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.testkit.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Subscriber;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SagaManagerNewAnnotationStyleTest extends Eventstore2TestKit{


    @Test
    public void test_that_methods_with_eventHandler_annotations_are_called_when_saga_receives_events() throws Exception {
        SagaInMemoryRepository sagaInMemoryRepository = new SagaInMemoryRepository();
        Props sagaManagerProps = Props.create(SagaManager.class, super.testActor(), sagaInMemoryRepository, super.testActor(), "no");
        TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, sagaManagerProps, UUID.randomUUID().toString());

        sagaManager.tell(new TestEvent("a test id"), super.testActor());
        //TODO: make sagaManagerFactory accept package path for scanning as a parmeter, and then fix this test so that the manager only scans this path
        expectMsgClass(Messages.Subscription.class);
        expectMsgClass(Messages.Subscription.class);
        expectMsgClass(TestEvent.class);
        assertEquals(Saga.STATE_FINISHED, sagaInMemoryRepository.getState("SagaWithNewAnotation", "a test id"));
    }

    @SagaEventIdProperty("testId")
    @Subscriber("TestAggregate")
    private static class SagaWithNewAnotation extends Saga {

        public SagaWithNewAnotation(String id, ActorRef commandDispatcher, SagaRepository repository) {
            super(id, commandDispatcher, repository);
        }

        @Override
        protected String getSagaStateId() {
            return "SagaWithNewAnotation";
        }

        @Handler
        public void handleEvent(TestEvent event){
            transitionState(STATE_FINISHED);
            commandDispatcher.tell(new TestEvent("We have been here"),self());
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

        @Override
        public String getAggregateRootId() {
            return null;
        }

        public String getTestId(){
            return testId;
        }

        @Override
    	public String getAggregateType() {
    		return "TestAggregate";
    	}
    }
}
