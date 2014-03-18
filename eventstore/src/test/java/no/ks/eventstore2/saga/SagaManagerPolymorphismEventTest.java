package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.response.Success;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class SagaManagerPolymorphismEventTest extends Eventstore2TestKit{


    @Test
    public void test_that_methods_with_eventHandler_annotations_are_called_when_saga_receives_events() throws Exception {
        SagaInMemoryRepository sagaInMemoryRepository = new SagaInMemoryRepository();
        Props sagaManagerProps = Props.create(SagaManager.class, super.testActor(), sagaInMemoryRepository, super.testActor(), "no");
        TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, sagaManagerProps, UUID.randomUUID().toString());

        sagaManager.tell(new SubEvent("a test id"), super.testActor());
        expectMsgClass(Subscription.class);
        expectMsgClass(Subscription.class);
        expectMsgClass(Success.class);
        assertEquals(Saga.STATE_FINISHED, sagaInMemoryRepository.getState(SagaHandlingASuperclassOfEvent.class, "a test id"));
    }

    @SagaEventIdProperty("testId")
    @Subscriber("TestAggregate")
    private static class SagaHandlingASuperclassOfEvent extends Saga {

        public SagaHandlingASuperclassOfEvent(String id, ActorRef commandDispatcher, SagaRepository repository) {
            super(id, commandDispatcher, repository);
        }

        @Handler
        public void handleEvent(SuperEvent event){
            transitionState(STATE_FINISHED);
            commandDispatcher.tell(new Success(), self());
        }
    }

    public abstract static class SuperEvent extends Event {
		private static final long serialVersionUID = 1L;

		private String testId;

        SuperEvent(String testId) {
            this.testId = testId;
        }

        public String getTestId() {
            return testId;
        }

        public void setTestId(String testId) {
            this.testId = testId;
        }
        
        @Override
    	public String getAggregateType() {
    		return "TestAggregate";
    	}
    }

    public static class SubEvent extends SuperEvent {
		private static final long serialVersionUID = 1L;

		SubEvent(String testId) {
            super(testId);
        }

        @Override
        public String getLogMessage() {
            return null;
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }
    }
}
