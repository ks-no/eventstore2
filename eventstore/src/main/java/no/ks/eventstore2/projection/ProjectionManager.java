package no.ks.eventstore2.projection;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import no.ks.eventstore2.TakeSnapshot;
import scala.concurrent.duration.Duration;

import java.util.*;

import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.resume;

public class ProjectionManager extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static final String IN_SUBSCRIBE = "insubscribe";
    public static final String SUBSCRIBE_FINISHED = "subscribe_finished";

    private Map<Class<? extends Projection>, ActorRef> projections = new HashMap<Class<? extends Projection>, ActorRef>();
    private Set<ActorRef> inSubscribePhase = new HashSet<>();
    private ActorRef errorListener;

    public static Props mkProps(ActorRef errorListener, List<Props> props){
        return Props.create(ProjectionManager.class,errorListener,props);
    }

    public ProjectionManager(ActorRef errorListener, List<Props> props){
        this.errorListener = errorListener;

        for (Props prop : props) {
            ActorRef projectionRef = getContext().actorOf(prop, prop.actorClass().getSimpleName());
            projections.put((Class<? extends Projection>) prop.actorClass(), projectionRef);
            inSubscribePhase.add(projectionRef);
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
        if(o instanceof ProjectionFailedError) {
            errorListener.tell(o,sender());
        } else if (o instanceof Call && "getProjectionRef".equals(((Call) o).getMethodName())) {
        	sender().tell(projections.get(((Call) o).getArgs()[0]), self());
        } else if (o instanceof Call && "isAnyoneInSubscribePhase".equals(((Call) o).getMethodName())) {
        	isAnyoneInSubscribePhase();
        } else if(o instanceof TakeSnapshot) {
            for (ActorRef actorRef : projections.values()) {
                actorRef.tell(o, sender());
            }
        } else if(IN_SUBSCRIBE.equals(o)){
            inSubscribePhase.add(sender());
        } else if(SUBSCRIBE_FINISHED.equals(o)){
            inSubscribePhase.remove(sender());
        }
    }

    private void isAnyoneInSubscribePhase() {
        if(!inSubscribePhase.isEmpty()) {
            log.info("projections in subscribe phase {}", inSubscribePhase);
        }
        sender().tell(new Boolean(!inSubscribePhase.isEmpty()), self());
    }
}
