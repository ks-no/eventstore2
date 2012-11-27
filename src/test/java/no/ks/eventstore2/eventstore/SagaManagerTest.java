package no.ks.eventstore2.eventstore;

import akka.actor.*;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.testImplementations.LetterReceived;
import no.ks.eventstore2.eventstore.testImplementations.NotificationSaga;
import no.ks.eventstore2.eventstore.testImplementations.NotificationSendt;
import no.ks.eventstore2.saga.SagaCompositeId;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManager;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;

public class SagaManagerTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    private SagaInMemoryRepository sagaInMemoryRepository;

    public SagaManagerTest() {
        super(_system);
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    public void testIncomingEventWithNewIdIsDispatchedToNewSaga() throws Exception {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManager");
        LetterReceived letterRecieved = new LetterReceived();
        letterRecieved.setAggregateId("notification");
        letterRecieved.setLetterId("1");
        ref.tell(letterRecieved, super.testActor());
        expectMsg("Send notification command");
    }

    @Test
    public void testIncomingEventWithExistingIdIsDispatchedToExistingSaga() throws Exception {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManager1");
        LetterReceived letterRecieved = new LetterReceived();
        letterRecieved.setAggregateId("notification");
        letterRecieved.setLetterId("2");
        ref.tell(letterRecieved, super.testActor());
        expectMsg("Send notification command");
        NotificationSendt notificationSendt = new NotificationSendt();
        notificationSendt.setAggregateId("notification");
        notificationSendt.setLetterId("2");
        ref.tell(notificationSendt, super.testActor());
        expectMsg("Update logs");
    }

    @Test
    public void testSagaStateSurvivesExceptionAndRestart() throws Exception {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManager2");
        LetterReceived letterRecieved = new LetterReceived();
        letterRecieved.setAggregateId("notification");
        letterRecieved.setLetterId("2");
        ref.tell(letterRecieved, super.testActor());
        ((HashMap <SagaCompositeId, ActorRef>)ReflectionTestUtils.getField(ref.underlyingActor(), "sagas")).get(new SagaCompositeId(NotificationSaga.class, "2")).tell("BOOM!", super.testActor());
        expectMsg("Send notification command");
        NotificationSendt notificationSendt = new NotificationSendt();
        notificationSendt.setAggregateId("notification");
        notificationSendt.setLetterId("2");
        ref.tell(notificationSendt, super.testActor());
        expectMsg("Update logs");
    }

    private Props getSagaManagerProps() {
        final ActorRef commandDispatcher = super.testActor();
        return new Props(new UntypedActorFactory(){
            @Override
            public Actor create() throws Exception {
                return new SagaManager(commandDispatcher, sagaInMemoryRepository);
            }
        });
    }
}
