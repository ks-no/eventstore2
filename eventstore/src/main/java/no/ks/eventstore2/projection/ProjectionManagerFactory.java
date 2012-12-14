package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.UntypedActorFactory;

import java.util.List;

public class ProjectionManagerFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	private final List<ProjectionFactory> projectionFactories;

    public ProjectionManagerFactory(List<ProjectionFactory> projectionFactories) {
        this.projectionFactories = projectionFactories;
    }

    public Actor create() throws Exception {
        return new ProjectionManager(projectionFactories);
    }
}
