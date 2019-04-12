package no.ks.eventstore2.saga;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import no.ks.eventstore2.testkit.Eventstore2TestKit;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SagaTest extends Eventstore2TestKit {

    private SagaInMemoryRepository sagaInMemoryRepository;

    SagaTest() {
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    void sagaChangesStateOnMessageRecieved() {
        final Props props = Props.create(FormProcess.class, UUID.randomUUID().toString(), super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_a");
        final FormProcess saga = ref.underlyingActor();

        ref.tell(ProtobufHelper.newEventWrapper(UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build()), null);

        assertEquals(2, saga.getState());
    }

    @Test
    void testSagaDispatchesCommandOnMessageRecieved() {
        final Props props = Props.create(FormProcess.class, UUID.randomUUID().toString(), super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_b");

        ref.tell(ProtobufHelper.newEventWrapper(UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build()), null);

        expectMsgClass(ParseForm.class);
    }

    @Test
    void testSagaPersistsState() {
        String sagaId = UUID.randomUUID().toString();
        final Props props = Props.create(FormProcess.class, sagaId, super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_c");

        ref.tell(ProtobufHelper.newEventWrapper(UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build()), null);

        assertEquals(2, sagaInMemoryRepository.getState("FormProcess", sagaId));
    }

    @Test
    void testSagaRestoresStateFromRepository() {
        String sagaId = UUID.randomUUID().toString();
        final Props props = Props.create(FormProcess.class, sagaId, super.testActor(), sagaInMemoryRepository);
        sagaInMemoryRepository.saveState("FormProcess", sagaId, (byte) 5);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_d");
        final FormProcess saga = ref.underlyingActor();
        assertEquals(5, saga.getState());
    }
}
