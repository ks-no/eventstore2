package no.ks.eventstore2.testkit;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.util.IdUtil;

public class EventStoreTestKit extends TestKit {

    protected static final ActorSystem actorSystem = ActorSystem.create("testSystem");

    public EventStoreTestKit() {
        super(actorSystem);
    }

    protected <T extends Actor> TestActorRef<T> createCommandHandler(Props props) {
        return TestActorRef.create(actorSystem, props, IdUtil.createUUID());
    }

    protected <T extends Actor> TestActorRef<T> createProjectionRef(Props props) {
        TestActorRef<T> actorRef = TestActorRef.create(actorSystem, props, IdUtil.createUUID());
        actorRef.tell(new CompleteSubscriptionRegistered(null), testActor());
        return actorRef;
    }

    protected <T extends Saga> T createSaga(Class<T> clz) {
        Props sagaProps = Props.create(clz, IdUtil.createUUID(), testActor(), new SagaInMemoryRepository());
        return (T) TestActorRef.create(actorSystem, sagaProps, IdUtil.createUUID()).underlyingActor();
    }

    protected EventReceiver createEventReceiver() {
        return (EventReceiver) TestActorRef.create(actorSystem, Props.create(EventReceiver.class), IdUtil.createUUID()).underlyingActor();
    }
}
