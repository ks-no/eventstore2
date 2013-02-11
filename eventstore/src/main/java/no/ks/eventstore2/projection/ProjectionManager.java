package no.ks.eventstore2.projection;

import akka.actor.*;
import akka.japi.Function;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.resume;

public class ProjectionManager extends UntypedActor {
    private Map<Class<? extends Projection>, ActorRef> projections = new HashMap<Class<? extends Projection>, ActorRef>();
    private ActorRef errorListener;

    public ProjectionManager(List<ProjectionFactory> projectionFactories, ActorRef errorListener) {
        this.errorListener = errorListener;

        for (ProjectionFactory projectionFactory : projectionFactories) {
            ActorRef projectionRef = getContext().actorOf(new Props(projectionFactory), projectionFactory.getProjectionClass().getSimpleName());
            projections.put(projectionFactory.getProjectionClass(), projectionRef);
        }
    }


    private static SupervisorStrategy strategy =
            new OneForOneStrategy(10, Duration.create("1 minute"),
                    new Function<Throwable, SupervisorStrategy.Directive>() {
                        public SupervisorStrategy.Directive apply(Throwable t) {
                            if (t instanceof RuntimeException || t instanceof Exception) {
                                return resume();
                            } else {
                                return restart();
                            }
                        }
                    });

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }


    @Override
    public void onReceive(Object o) throws Exception {
        if(o instanceof ProjectionFailedError){
            errorListener.tell(o,sender());
        }else if (o instanceof Call && "getProjectionRef".equals(((Call) o).getMethodName()))
            sender().tell(projections.get(((Call) o).getArgs()[0]), self());
    }
}
