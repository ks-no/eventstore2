package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import eventstore.Messages;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class TiemoutSagaTest extends TestKit{

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private SagaInMemoryRepository sagaInMemoryRepository;

    public TiemoutSagaTest() {
        super(_system);
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    public void schedulesAwake() {
        final ActorRef commandDispatcher = super.testActor();
        final Props sagaprop = Props.create(SagaTimerUt.class, "sagaid", commandDispatcher, sagaInMemoryRepository);

        final TestActorRef<SagaTimerUt> ref = TestActorRef.create(_system, sagaprop, super.testActor(), "sagatimerut");
        final SagaTimerUt saga = ref.underlyingActor();
        ref.tell(new FormParsed("1"), super.testActor());
        expectMsgClass(Messages.ScheduleAwake.class);
        ref.tell("awake", super.testActor());
        assertEquals(Saga.STATE_FINISHED, saga.getState());
    }

}
