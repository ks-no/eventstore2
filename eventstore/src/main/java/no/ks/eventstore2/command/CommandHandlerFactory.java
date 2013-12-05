package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import akka.routing.RouterConfig;

@Deprecated
public abstract class CommandHandlerFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	protected CommandHandlerFactory(ActorRef eventStore) {
		this.eventStore = eventStore;
	}

	protected CommandHandlerFactory() {
	}

	public void setEventStore(ActorRef eventStore) {
        this.eventStore = eventStore;
    }

    protected ActorRef eventStore;

	public boolean useRouter() {return false;}

	public RouterConfig getRouter() {
		return new RoundRobinRouter(1);
	}
}
