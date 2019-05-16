package no.ks.eventstore2.projection;

import akka.ConfigurationException;
import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.DeciderBuilder;
import akka.serialization.Serialization;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import eventstore.Messages;
import io.vavr.collection.HashMultimap;
import io.vavr.collection.Multimap;
import no.ks.eventstore2.RestartActorException;
import no.ks.eventstore2.TakeSnapshot;
import scala.concurrent.duration.Duration;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.resume;

public class ProjectionManager extends AbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static final String SUBSCRIBE_FINISHED = "subscribe_finished";

    private Map<Class<? extends AbstractActor>, ActorRef> projections = new HashMap<>();
    private Set<ActorRef> inSubscribePhase = new HashSet<>();
    private Set<ActorRef> liveSubscriptions = new HashSet<>();

    private Multimap<String, ActorRef> categoryProjectionMap = HashMultimap.withSet().empty();
    private Map<ActorRef, Cache<String, Long>> projectionHwm = new HashMap<>();
    private List<AwaitProjections> awaitingWrites = new LinkedList<>();

    private ActorRef errorListener;
    private Cluster cluster;

    public static Props mkProps(ActorRef errorListener, List<Props> props) {
        return Props.create(ProjectionManager.class, errorListener, props);
    }

    @SuppressWarnings("unchecked")
    public ProjectionManager(ActorRef errorListener, List<Props> props) {
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
    public Receive createReceive() {
        return receiveBuilder()
                .match(Call.class, call -> "getProjectionRef".equals(call.getMethodName()), this::handleGetProjectionRef)
                .match(Call.class, call -> "isAnyoneInSubscribePhase".equals(call.getMethodName()), call -> this.handleIsAnyoneInSubscribePhase())
                .match(ClusterEvent.ReachableMember.class, this::handleReachableMember)
                .match(Messages.EventProcessed.class, this::handleEventProcessed)
                .match(Messages.AcknowledgePreviousEventsProcessed.class, this::handleAcknowledgePreviousEventsProcessed)
                .match(TakeSnapshot.class, this::handleTakeSnapshot)
                .match(Messages.GetSubscribers.class, o -> this.handleGetSubscribers())
                .match(Messages.Subscription.class, this::handleSubscription)
                .matchEquals(SUBSCRIBE_FINISHED, o -> this.handleSubscribeFinished())
                .matchEquals("getProjections", o -> this.handleGetProjections())
                .matchEquals("restart", o -> this.handleRestart())
                .build();
    }

    private void handleRestart() {
        for (ActorRef actorRef : projections.values()) {
            actorRef.tell("restart", self());
            log.debug("Sending restart to actorref {}", actorRef);
        }
    }

    private void handleReachableMember(ClusterEvent.ReachableMember event) {
        log.info("Member reachable: {}", event.member());

        final Iterable<Member> members = cluster.state().getMembers();
        Member oldest = event.member();
        for (Member member : members) {
            if(member.isOlderThan(oldest)){
                oldest = member;
            }
        }
        log.info("Member oldest {}", oldest );
        if (oldest.equals(event.member())) {
            for (ActorRef actorRef : projections.values()) {
                actorRef.tell("restart", self());
                log.debug("Sending restart to actorref {}", actorRef);
            }
        }
    }

    private void handleTakeSnapshot(TakeSnapshot takeSnapshot) {
        for (ActorRef actorRef : projections.values()) {
            actorRef.tell(takeSnapshot, sender());
        }
    }

    private void handleGetSubscribers() {
        final Messages.Subscribers subscribers = Messages.Subscribers.newBuilder()
                .addAllLiveSubscriptions(liveSubscriptions.stream().map(Serialization::serializedActorPath).collect(Collectors.toList()))
                .addAllInSubscribePhase(inSubscribePhase.stream().map(Serialization::serializedActorPath).collect(Collectors.toList()))
                .build();
        sender().tell(subscribers, self());
    }

    private void handleGetProjectionRef(Call call) {
        sender().tell(projections.get(call.getArgs()[0]), self());
    }

    private void handleIsAnyoneInSubscribePhase() {
        isAnyoneInSubscribePhase();
    }

    private void handleSubscription(Messages.Subscription subscription) {
        inSubscribePhase.add(sender());
        if (subscription.getShouldNotifyProjectionManager()) {
            log.info("{} is set up to notify ProjectionManager", sender());
            categoryProjectionMap = categoryProjectionMap.put(subscription.getCategory(), sender());
            projectionHwm.put(sender(), CacheBuilder.newBuilder()
                    .expireAfterWrite(java.time.Duration.ofMinutes(1))
                    .maximumSize(1000)
                    .build());
        }
    }

    private void handleSubscribeFinished() {
        inSubscribePhase.remove(sender());
        liveSubscriptions.add(sender());
    }

    private void handleGetProjections() {
        sender().tell(projections, self());
    }

    private void isAnyoneInSubscribePhase() {
        if(!inSubscribePhase.isEmpty()) {
            log.info("projections in subscribe phase {}", inSubscribePhase);
        }
        sender().tell(new Boolean(!inSubscribePhase.isEmpty()), self());
    }

    private void handleEventProcessed(Messages.EventProcessed message) {
        if (!projectionHwm.containsKey(sender())) {
            throw new RuntimeException(String.format("Got EventProcessed from unknown sender: %s", sender()));
        }
        projectionHwm.get(sender()).put(message.getPosition().getAggregateId(), message.getPosition().getEventNumber());

        Iterator<AwaitProjections> iterator = awaitingWrites.iterator();
        while (iterator.hasNext()) {
            AwaitProjections await = iterator.next();
            if (areProjectionsUpdated(await.getCategory(), await.getAwaitedPosition())) {
                log.debug("Sending response for {}", await);
                await.getAwaitingActor().tell(Messages.PreviousEventsProcessed.getDefaultInstance(), self());
                iterator.remove();
            } else if (Instant.now().toEpochMilli() >= await.getExpiresAt()) {
                log.debug("Await timed out for {}", await);
                iterator.remove();
            }
        }
    }

    private void handleAcknowledgePreviousEventsProcessed(Messages.AcknowledgePreviousEventsProcessed message) {
        if (areProjectionsUpdated(message.getCategory(), message.getAwaitPosition())) {
            sender().tell(Messages.PreviousEventsProcessed.getDefaultInstance(), self());
        } else {
            awaitingWrites.add(new AwaitProjections(message.getCategory(), Instant.now().plusSeconds(5).toEpochMilli(), message.getAwaitPosition(), sender()));
        }
    }

    private boolean areProjectionsUpdated(String category, Messages.StreamPosition awaitedPosition) {
        if (categoryProjectionMap.containsKey(category)) {
            return categoryProjectionMap.get(category).get().forAll(projection -> {
                Cache<String, Long> cache = projectionHwm.get(projection);
                if (cache != null) {
                    Long hwm = cache.getIfPresent(awaitedPosition.getAggregateId());
                    return hwm != null && hwm >= awaitedPosition.getEventNumber();
                }
                throw new RuntimeException(String.format("Could not find hwm cache for %s", projection));
            });
        }
        return true;
    }
}
