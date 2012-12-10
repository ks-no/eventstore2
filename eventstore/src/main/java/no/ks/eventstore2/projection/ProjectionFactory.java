package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

public abstract class ProjectionFactory implements UntypedActorFactory {
    protected final ActorRef eventstore;

    protected ProjectionFactory(ActorRef eventstore) {
        this.eventstore = eventstore;
    }

    public abstract Class<? extends Projection> getProjectionClass();
}
