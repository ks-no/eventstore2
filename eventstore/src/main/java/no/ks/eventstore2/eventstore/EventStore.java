package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.dispatch.OnFailure;
import com.google.common.collect.HashMultimap;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.text.SimpleDateFormat;
import java.util.HashSet;

import static akka.dispatch.Futures.future;

public class EventStore extends UntypedActor {

    public static final String EVENTSTOREMESSAGES = "eventstoremessages";

    private static Logger log = LoggerFactory.getLogger(EventStore.class);

    private HashMultimap<String, ActorRef> aggregateSubscribers = HashMultimap.create();

    JournalStorage storage;

    private ActorRef eventstoresingeltonProxy;
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    public static Props mkProps(JournalStorage journalStorage) {

        return Props.create(EventStore.class, journalStorage);
    }

    public EventStore(JournalStorage journalStorage) {
        storage = journalStorage;
    }

    @Override
    public void preStart() {

        try {
            Cluster cluster = Cluster.get(getContext().system());
            cluster.subscribe(self(), ClusterEvent.MemberRemoved.class);
            log.info("{} subscribes to cluster events", self());
        } catch (ConfigurationException e) {
        }
        final ActorSystem system = context().system();
        try {
            Cluster cluster = Cluster.get(system);
            final ClusterSingletonManagerSettings settings =
                    ClusterSingletonManagerSettings.create(system);
            system.actorOf(ClusterSingletonManager.props(
                    EventstoreSingelton.mkProps(storage),
                    "shutdown", settings), "eventstoresingelton");

            ClusterSingletonProxySettings proxySettings =
                    ClusterSingletonProxySettings.create(system);
            proxySettings.withBufferSize(10000);

            eventstoresingeltonProxy = system.actorOf(ClusterSingletonProxy.props("/user/eventstoresingelton", proxySettings),
                    "eventstoresingeltonProxy");
        } catch (ConfigurationException e) {
            log.info("not cluster system");
            eventstoresingeltonProxy = system.actorOf(EventstoreSingelton.mkProps(storage));
        }
        log.debug("Eventstore started with adress {}", getSelf().path());
    }


    public void onReceive(Object o) throws Exception {
        log.debug("Received message {}", o);
        try {
            if (o instanceof String && "fail".equals(o)) {
                eventstoresingeltonProxy.tell(o, sender());
            }
            if (o instanceof ClusterEvent.MemberRemoved) {
                ClusterEvent.MemberRemoved removed = (ClusterEvent.MemberRemoved) o;
                log.info("Member removed: {} status {}", removed.member(), removed.previousStatus());
                for (String aggregate : aggregateSubscribers.keySet()) {
                    HashSet<ActorRef> remove = new HashSet<ActorRef>();
                    for (ActorRef actorRef : aggregateSubscribers.get(aggregate)) {
                        if (actorRef.path().address().equals(removed.member().address())) {
                            remove.add(actorRef);
                            log.debug("removeing actorref {}", actorRef);
                        }
                    }
                    for (ActorRef actorRef : remove) {
                        aggregateSubscribers.get(aggregate).remove(actorRef);
                        log.info("Aggregate {} removeed subscriber {}", aggregate, actorRef);
                    }
                }
            } else if (o instanceof Subscription) {
                Subscription subscription = (Subscription) o;
                tryToFillSubscription(sender(), subscription);
            } else if (o instanceof String ||
                    o instanceof Subscription ||
                    o instanceof StoreEvents ||
                    o instanceof Event ||
                    o instanceof RetreiveAggregateEvents ||
                    o instanceof AcknowledgePreviousEventsProcessed ||
                    o instanceof UpgradeAggregate ||
                    o instanceof TakeBackup ||
                    o instanceof TakeSnapshot) {
                if (!(o instanceof AcknowledgePreviousEventsProcessed || o instanceof RetreiveAggregateEvents))
                    log.info("Sending to singelton  message {} from {}", o, sender());
                eventstoresingeltonProxy.tell(o, sender());
            }
        } catch (Exception e) {
            log.error("Eventstore got an error: ", e);
            throw e;
        }
    }

    private void tryToFillSubscription(final ActorRef sender, final Subscription subscription) {
        final ActorRef self = self();

        if (subscription instanceof AsyncSubscription) {
            Future<Boolean> f = future(() -> {
                log.info("Got async subscription on {} from {}, filling subscriptions", subscription, sender);

                boolean finished = loadEvents(sender, subscription);

                if (!finished) {
                    log.info("Async IncompleteSubscriptionPleaseSendNew");
                    sender.tell(new IncompleteSubscriptionPleaseSendNew(subscription.getAggregateType()), self);
                } else {
                    log.info("Async CompleteAsyncSubscriptionPleaseSendSyncSubscription");
                    sender.tell(new CompleteAsyncSubscriptionPleaseSendSyncSubscription(subscription.getAggregateType()), self);
                }
                return finished;
            }, getContext().system().dispatcher());
            f.onFailure(new OnFailure() {
                public void onFailure(Throwable failure) {
                    log.error("Error in AsyncSubscribe, restarting subscriber", failure);
                    sender.tell(new NewEventstoreStarting(), self);
                }
            }, getContext().system().dispatcher());
        } else {
            log.info("Sending subscription to singelton {} from {}", eventstoresingeltonProxy.path(), sender().path());
            eventstoresingeltonProxy.tell(subscription, sender());
        }
    }

    private boolean loadEvents(final ActorRef sender, Subscription subscription) {
        boolean finished = false;
        if (subscription.getFromJournalId() == null || "".equals(subscription.getFromJournalId().trim())) {
            finished = storage.loadEventsAndHandle(subscription.getAggregateType(), new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    sendEvent(event, sender);
                }
            });
        } else {
            finished = storage.loadEventsAndHandle(subscription.getAggregateType(), new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    sendEvent(event, sender);
                }
            }, subscription.getFromJournalId());

        }
        return finished;
    }

    private void sendEvent(Event event, ActorRef subscriber) {
        Event upgadedEvent = upgradeEvent(event);
        log.debug("Publishing event {} to {}", upgadedEvent, subscriber);
        subscriber.tell(upgadedEvent, self());
    }

    private Event upgradeEvent(Event event) {
        Event currentEvent = event;
        Event upgraded = currentEvent.upgrade();
        while (upgraded != currentEvent) {
            currentEvent = upgraded;
            upgraded = currentEvent.upgrade();
        }
        return upgraded;
    }

    private void addSubscriber(Subscription subscription) {
        aggregateSubscribers.put(subscription.getAggregateType(), sender());
        log.info("Added subscriber {} " + subscription);
        log.debug("Current subscribers " + toLogString(aggregateSubscribers));
    }

    private String toLogString(HashMultimap<String, ActorRef> aggregateSubscribers) {
        String str = "";
        for (String s : aggregateSubscribers.keySet()) {
            str += "['" + s + "':" + aggregateSubscribers.get(s) + "]\n";
        }
        return str;
    }


}
