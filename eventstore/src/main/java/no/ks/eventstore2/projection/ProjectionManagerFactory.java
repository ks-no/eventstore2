package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.UntypedActorFactory;

import java.util.List;

public class ProjectionManagerFactory implements UntypedActorFactory {
    private final List<ProjectionFactory> projectionFactories;

    public ProjectionManagerFactory(List<ProjectionFactory> projectionFactories) {
        this.projectionFactories = projectionFactories;
    }

    @Override
    public Actor create() throws Exception {
        return new ProjectionManager(projectionFactories);
    }
}
