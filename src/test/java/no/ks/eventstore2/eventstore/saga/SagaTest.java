package no.ks.eventstore2.eventstore.saga;

import akka.actor.*;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.testImplementations.LetterReceived;
import no.ks.eventstore2.eventstore.testImplementations.NotificationSaga;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SagaTest extends TestKit {


    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private SagaInMemoryRepository sagaInMemoryRepository;

    public SagaTest() {
        super(_system);
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    public void sagaChangesStateOnMessageRecieved() {
        final Props props = getNotificationSagaProps();
        final TestActorRef<NotificationSaga> ref = TestActorRef.create(_system, props, "not_saga_a");
        final NotificationSaga saga = ref.underlyingActor();
        ref.tell(new LetterReceived(), null);
        assertEquals(2, saga.getState());
    }

    @Test
    public void testSagaDispatchesCommandOnMessageRecieved() throws Exception {
        final Props props = getNotificationSagaProps();
        final TestActorRef<NotificationSaga> ref = TestActorRef.create(_system, props, "not_saga_b");
        ref.tell(new LetterReceived(), super.testActor());
        expectMsg("Send notification command");
    }

    @Test
    public void testSagaPersistsState() throws Exception {
        final Props props = getNotificationSagaProps();
        final TestActorRef<NotificationSaga> ref = TestActorRef.create(_system, props, "not_saga_c");
        ref.tell(new LetterReceived(), super.testActor());
        assertEquals(2, sagaInMemoryRepository.getState(NotificationSaga.class, "123"));
    }

    @Test
    public void testSagaRestoresStateFromRepository() throws Exception {
        final Props props = getNotificationSagaProps("123123");
        sagaInMemoryRepository.saveState(NotificationSaga.class, "123123", (byte) 5);
        final TestActorRef<NotificationSaga> ref = TestActorRef.create(_system, props, "not_saga_d");
        final NotificationSaga saga = ref.underlyingActor();
        assertEquals(5, saga.getState());
    }

    private Props getNotificationSagaProps(final String sagaId) {
        final ActorRef commandDispatcher = super.testActor();
        return new Props(new UntypedActorFactory(){
			public Actor create() throws Exception {
                return new NotificationSaga(sagaId, commandDispatcher, sagaInMemoryRepository);
            }
        });
    }


    private Props getNotificationSagaProps() {
        return getNotificationSagaProps("123");
    }
}
