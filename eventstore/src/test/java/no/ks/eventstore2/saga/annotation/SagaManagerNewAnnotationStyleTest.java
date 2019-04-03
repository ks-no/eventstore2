package no.ks.eventstore2.saga.annotation;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManager;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class SagaManagerNewAnnotationStyleTest extends EventstoreEventstore2TestKit {

    @Test
    void test_that_methods_with_eventHandler_annotations_are_called_when_saga_receives_events() {
        SagaInMemoryRepository sagaInMemoryRepository = new SagaInMemoryRepository();
        Props sagaManagerProps = Props.create(SagaManager.class, super.testActor(), sagaInMemoryRepository, eventstoreConnection, "no.ks.eventstore2.saga.annotation");
        TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, sagaManagerProps, UUID.randomUUID().toString());

        String aggregateRootId = UUID.randomUUID().toString();
        journal.saveEvent(ProtobufHelper.newEventWrapper("Test", aggregateRootId,
                EventstoreTest.TestEvent.newBuilder().setMessage(UUID.randomUUID().toString()).build()));

        EventstoreTest.TestEvent testEvent = expectMsgClass(EventstoreTest.TestEvent.class);
        assertThat(testEvent.getMessage(), is("EventstoreTest.TestEvent received"));
        new TestInvoker().invoke(() -> assertThat(Saga.STATE_FINISHED, is(sagaInMemoryRepository.getState("SagaWithNewAnotation", aggregateRootId))));
    }
}
