package no.ks.eventstore2.projection;

import akka.ConfigurationException;
import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import akka.japi.pf.DeciderBuilder;
import no.ks.eventstore2.RestartActorException;
import no.ks.eventstore2.TakeSnapshot;
import scala.concurrent.duration.Duration;

import java.util.*;

import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.resume;

public class ProjectionManager extends UntypedAbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static final String IN_SUBSCRIBE = "insubscribe";
    public static final String SUBSCRIBE_FINISHED = "subscribe_finished";

    private Map<Class<? extends AbstractActor>, ActorRef> projections = new HashMap<>();
    private Set<ActorRef> inSubscribePhase = new HashSet<>();
    private ActorRef errorListener;
    private Cluster cluster;

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

    public static void restartManager(ActorRef projectionManager){
        projectionManager.tell("restart", ActorRef.noSender());
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        try {
            cluster = Cluster.get(getContext().system());
            cluster.subscribe(self(), ClusterEvent.ReachableMember.class);
            log.info("{} subscribes to cluster events", self());
        } catch (ConfigurationException e) {
        }
    }

    private SupervisorStrategy strategy =
            new OneForOneStrategy(10, Duration.create("1 minute"), DeciderBuilder
                    .match(RestartActorException.class, e -> restart())
                    .match(ProjectionFailedException.class, e -> {
                        errorListener.tell(e.getError(), sender());
                        return resume();
                    })
                    .match(Exception.class, e -> resume())
                    .matchAny(e -> restart())
                    .build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if ("restart".equals(o)) {
            for (ActorRef actorRef : projections.values()) {
                actorRef.tell("restart", self());
                log.debug("Sending restart to actorref {}", actorRef);
            }
        } else if (o instanceof ClusterEvent.ReachableMember) {
            ClusterEvent.ReachableMember reachable = (ClusterEvent.ReachableMember) o;
            log.info("Member reachable: {}", reachable.member());

            final Iterable<Member> members = cluster.state().getMembers();
            Member oldest = reachable.member();
            for (Member member : members) {
                if(member.isOlderThan(oldest)){
                    oldest = member;
                }
            }
            log.info("Member oldest {}", oldest );
            if (oldest.equals(reachable.member())) {
                for (ActorRef actorRef : projections.values()) {
                    actorRef.tell("restart", self());
                    log.debug("Sending restart to actorref {}", actorRef);
                }
            }
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
        } else if("getProjections".equals(o)){
            sender().tell(projections, self());
        }
    }

    private void isAnyoneInSubscribePhase() {
        if(!inSubscribePhase.isEmpty()) {
            log.info("projections in subscribe phase {}", inSubscribePhase);
        }
        sender().tell(new Boolean(!inSubscribePhase.isEmpty()), self());
    }
}
