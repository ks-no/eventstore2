package no.ks.eventstore2.saga.annotation;

import akka.actor.ActorRef;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaEventIdProperty;
import no.ks.eventstore2.saga.SagaRepository;

@SagaEventIdProperty(useAggregateRootId = true)
@Subscriber("no.ks.events.svarut.Test")
public class SagaWithNewAnnotation extends Saga {

    SagaWithNewAnnotation(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Override
    protected String getSagaStateId() {
        return "SagaWithNewAnotation";
    }

    @Handler
    public void handleEvent(EventstoreTest.TestEvent event) {
        transitionState(STATE_FINISHED);
        commandDispatcher.tell("EventstoreTest.TestEvent received", self());
    }
}
