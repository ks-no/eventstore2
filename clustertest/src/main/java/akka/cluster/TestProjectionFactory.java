package akka.cluster;

import akka.actor.Actor;
import akka.actor.ActorRef;
import no.ks.eventstore2.projection.Projection;
import no.ks.eventstore2.projection.ProjectionFactory;

public class TestProjectionFactory extends ProjectionFactory {

    protected TestProjectionFactory(ActorRef eventstore) {
        super(eventstore);
    }

    @Override
    public Class<? extends Projection> getProjectionClass() {
        return TestProjection.class;
    }

    /**
     * This method must return a different instance upon every call.
     */
            @Override
            public Actor create() {
        return new TestProjection(eventstore);
    }
}
