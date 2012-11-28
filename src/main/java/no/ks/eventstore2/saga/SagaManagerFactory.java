package no.ks.eventstore2.saga;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

public class SagaManagerFactory implements UntypedActorFactory {

    private SagaRepository repository;
    private ActorRef commandDispatcher;
    private ActorRef eventstore;

    public SagaManagerFactory(SagaRepository repository, ActorRef commandDispatcher, ActorRef eventstore) {
        this.repository = repository;
        this.commandDispatcher = commandDispatcher;
        this.eventstore = eventstore;
    }

    @Override
    public Actor create() throws Exception {
        return new SagaManager(commandDispatcher, repository, eventstore);
    }
}
