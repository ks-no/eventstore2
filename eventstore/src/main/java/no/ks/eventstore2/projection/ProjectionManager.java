package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectionManager extends UntypedActor {
    private Map<Class<? extends Projection>, ActorRef> projections = new HashMap<Class<? extends Projection>, ActorRef>();
    public ProjectionManager(List<ProjectionFactory> projectionFactories) {
        for (ProjectionFactory projectionFactory : projectionFactories) {
            ActorRef projectionRef = getContext().actorOf(new Props(projectionFactory), projectionFactory.getProjectionClass().getSimpleName());
            projections.put(projectionFactory.getProjectionClass(), projectionRef);
        }
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof Call && "getProjectionRef".equals(((Call) o).getMethodName()))
            sender().tell(projections.get(((Call) o).getArgs()[0]), self());
    }
}
