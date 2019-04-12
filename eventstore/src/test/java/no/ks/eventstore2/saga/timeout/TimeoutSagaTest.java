package no.ks.eventstore2.saga.timeout;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import eventstore.Messages;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.testkit.Eventstore2TestKit;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.util.AssertionErrors.assertTrue;


class TimeoutSagaTest extends Eventstore2TestKit {

    private SagaInMemoryRepository sagaInMemoryRepository;

    TimeoutSagaTest() {
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    void schedulesAwake() {
        final ActorRef commandDispatcher = super.testActor();
        final Props sagaprop = Props.create(SagaTimerUt.class, "sagaid", commandDispatcher, sagaInMemoryRepository);

        final TestActorRef<SagaTimerUt> ref = TestActorRef.create(_system, sagaprop, super.testActor(), "sagatimerut");
        final SagaTimerUt saga = ref.underlyingActor();

        ref.tell(ProtobufHelper.newEventWrapper(UUID.randomUUID().toString(), EventStoreForm.FormParsed.newBuilder().setFormId(UUID.randomUUID().toString()).build()), null);

        expectMsgClass(Messages.ScheduleAwake.class);
        ref.tell("awake", super.testActor());
        Assertions.assertEquals(Saga.STATE_FINISHED, saga.getState());
    }

    @Test
    void schedulesAwakeInTheFuture() { // TODO: What does this do?
        final ActorRef commandDispatcher = super.testActor();
        final Props sagaprop = Props.create(SagaTimerUt.class, "sagaid", commandDispatcher, sagaInMemoryRepository);

        final TestActorRef<SagaTimerUt> ref = TestActorRef.create(_system, sagaprop, super.testActor(), "sagatimerut");
        final SagaTimerUt saga = ref.underlyingActor();

        ref.tell(ProtobufHelper.newEventWrapper(UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build()), null);

        final Messages.ScheduleAwake receive = (Messages.ScheduleAwake) receiveOne(Duration.create(3, TimeUnit.SECONDS));

        assertTrue("Correct timeout value", receive.getAwake().getSeconds() - DateTime.now().plusSeconds(120).toDate().getTime()/1000 < 5);
    }

}
