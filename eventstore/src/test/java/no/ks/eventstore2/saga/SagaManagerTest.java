package no.ks.eventstore2.saga;

import akka.actor.*;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.IncompleteSubscriptionPleaseSendNew;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.formProcessorProject.*;
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

        FormReceived formReceived = new FormReceived("1");
        formReceived.setAggregateId("FORM");
        ref.tell(formReceived, super.testActor());
        expectMsgClass(ParseForm.class);
    }

    @Test
    public void testIncomingEventWithExistingIdIsDispatchedToExistingSaga() throws Exception {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManager1");
        FormReceived formReceived = new FormReceived("2");
        formReceived.setAggregateId("FORM");
        ref.tell(formReceived, super.testActor());
        expectMsgClass(ParseForm.class);
        FormParsed formParsed = new FormParsed("2");
        formParsed.setAggregateId("FORM");
        ref.tell(formParsed, super.testActor());
        expectMsgClass(DeliverForm.class);
    }

    @Test
    public void testSagaStateSurvivesExceptionAndRestart() throws Exception {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManager2");
        FormReceived formReceived = new FormReceived("2");
        formReceived.setAggregateId("FORM");
        ref.tell(formReceived, super.testActor());
        ((HashMap <SagaCompositeId, ActorRef>)ReflectionTestUtils.getField(ref.underlyingActor(), "sagas")).get(new SagaCompositeId(FormProcess.class, "2")).tell("BOOM!", super.testActor());
        expectMsgClass(ParseForm.class);
        FormParsed formParsed = new FormParsed("2");
        formParsed.setAggregateId("FORM");
        ref.tell(formParsed, super.testActor());
        expectMsgClass(DeliverForm.class);
    }

    private Props getSagaManagerProps() {
        final ActorRef testActor = super.testActor();
        return new Props(new UntypedActorFactory(){
			public Actor create() throws Exception {
                return new SagaManager(testActor, sagaInMemoryRepository, _system.actorOf(new Props(DummyActor.class)), "no");
            }
        });
    }

    @Test
    public void testIncompleteSubscribeSendsCorrectJournalid() throws Exception {
        final Props props = SagaManager.mkProps(super.testActor(), sagaInMemoryRepository, super.testActor(), "no");
        final TestActorRef<SagaManager> ref = TestActorRef.create(_system, props, "sagaManagerIncomplete");
        expectMsgClass(Subscription.class);
        expectMsgClass(Subscription.class);
        FormReceived msg = new FormReceived("3");
        msg.setJournalid("01");
        ref.tell(msg, super.testActor());
        expectMsgClass(ParseForm.class);
        ref.tell(new IncompleteSubscriptionPleaseSendNew(msg.getAggregateId()),super.testActor());
        expectMsg(new Subscription(msg.getAggregateId(),"01"));
    }
}
