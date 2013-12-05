package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

import java.util.List;

@Deprecated
public class ProjectionManagerFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	private final List<ProjectionFactory> projectionFactories;
    private ActorRef errorListener;

    public ProjectionManagerFactory(List<ProjectionFactory> projectionFactories, ActorRef errorListener) {
        this.projectionFactories = projectionFactories;
        this.errorListener = errorListener;
    }

    public Actor create() throws Exception {
        return new ProjectionManager(projectionFactories, errorListener);
    }
}
