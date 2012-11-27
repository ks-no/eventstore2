package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

public abstract class CommandHandlerFactory implements UntypedActorFactory {

    public void setEventStore(ActorRef eventStore) {
        this.eventStore = eventStore;
    }

    protected ActorRef eventStore;
}
