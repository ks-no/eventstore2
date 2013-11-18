package no.ks.eventstore2.saga;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

public class SagaManagerFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	private SagaRepository repository;
    private ActorRef commandDispatcher;
    private ActorRef eventstore;
    private String packageScanPath;

    public SagaManagerFactory(SagaRepository repository, ActorRef commandDispatcher, ActorRef eventstore) {
       this(repository, commandDispatcher, eventstore, "no");
    }

    public SagaManagerFactory(SagaRepository repository, ActorRef commandDispatcher, ActorRef eventstore, String packageScanPath) {
        this.repository = repository;
        this.commandDispatcher = commandDispatcher;
        this.eventstore = eventstore;
        this.packageScanPath = packageScanPath;
    }

    public Actor create() throws Exception {
        return new SagaManager(commandDispatcher, repository, eventstore, packageScanPath);
    }
}
