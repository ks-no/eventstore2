package no.ks.eventstore2.saga;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

public class SagaFactory implements UntypedActorFactory {
    private final Class<? extends Saga> clz;
    private final ActorRef commandDispatcher;
    private final SagaRepository repository;
    private String sagaId;

    public SagaFactory(Class<? extends Saga> clz, ActorRef commandDispatcher, SagaRepository repository, String sagaId) {
        this.clz = clz;
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
        this.sagaId = sagaId;
    }

    @Override
    public Actor create() throws Exception {
        return clz.getConstructor(String.class, ActorRef.class, SagaRepository.class).newInstance(sagaId, commandDispatcher, repository);
    }
}
