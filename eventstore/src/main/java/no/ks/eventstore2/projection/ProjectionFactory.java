package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

@Deprecated
public abstract class ProjectionFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	protected ActorRef eventstore;

    protected ProjectionFactory(ActorRef eventstore) {
        this.eventstore = eventstore;
    }

	protected ProjectionFactory() {
	}

	public ActorRef getEventstore() {
		return eventstore;
	}

	public void setEventstore(ActorRef eventstore) {
		this.eventstore = eventstore;
	}

	public abstract Class<? extends Projection> getProjectionClass();
}
