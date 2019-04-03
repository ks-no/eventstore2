package no.ks.eventstore2.saga;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SagaTest extends TestKit {

    private static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory.load().getConfig("TestSys"));
    private SagaInMemoryRepository sagaInMemoryRepository;

    SagaTest() {
        super(_system);
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    void sagaChangesStateOnMessageRecieved() {
        final Props props = Props.create(FormProcess.class, UUID.randomUUID().toString(), super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_a");
        final FormProcess saga = ref.underlyingActor();

        ref.tell(EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build(), null);

        assertEquals(2, saga.getState());
    }

    @Test
    void testSagaDispatchesCommandOnMessageRecieved() {
        final Props props = Props.create(FormProcess.class, UUID.randomUUID().toString(), super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_b");

        ref.tell(EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build(), null);

        expectMsgClass(ParseForm.class);
    }

    @Test
    void testSagaPersistsState() {
        String sagaId = UUID.randomUUID().toString();
        final Props props = Props.create(FormProcess.class, sagaId, super.testActor(), sagaInMemoryRepository);
        final TestActorRef<FormProcess> ref = TestActorRef.create(_system, props, "not_saga_c");

        ref.tell(EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build(), null);

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
