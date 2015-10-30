package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import com.google.common.collect.HashMultimap;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.response.Success;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class EventstoreSingelton extends UntypedActor {

    private Logger log = LoggerFactory.getLogger(EventstoreSingelton.class);

    private HashMultimap<String, ActorRef> aggregateSubscribers = HashMultimap.create();

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    public static Props mkProps(JournalStorage journal) {
        return Props.create(EventstoreSingelton.class, journal);
    }

    JournalStorage storage;

    public EventstoreSingelton(JournalStorage storage) {
        this.storage = storage;
    }

    @Override
    public void preStart() throws Exception {
        log.debug("EventstoreSingelton started with adress {}", getSelf().path());
        try {
            Cluster cluster = Cluster.get(getContext().system());
            cluster.subscribe(self(), ClusterEvent.MemberRemoved.class);
            log.info("{} subscribes to cluster events", self());
        } catch (ConfigurationException e) {
        }
        storage.open();
        try {
            ActorRef mediator =
                    DistributedPubSub.get(getContext().system()).mediator();
            mediator.tell(new DistributedPubSubMediator.Publish(EventStore.EVENTSTOREMESSAGES, new NewEventstoreStarting()), self());
            log.info("Sent message new eventstore starting");
        } catch(ConfigurationException e){
            log.info("No subscribe to eventstore messages in non cluster system");
        }
        super.preStart();
    }

    @Override
    public void postStop() throws Exception {
        storage.close();
        super.postStop();
    }


    @Override
    public void postRestart(Throwable reason) throws Exception {
        log.warn("Restarted eventstoreSingelton, restarting storage");
        storage.close();
        super.postRestart(reason);
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof String && "fail".equals(o)) {
            throw new RuntimeException("Failing by force");
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
        } else if(o instanceof Subscription){
            tryToFillSubscription(sender(), (Subscription) o);
        } else if (o instanceof StoreEvents) {
            storeEvents((StoreEvents) o);
            publishEvents((StoreEvents) o);
            for (Event event : ((StoreEvents) o).getEvents()) {
                log.info("Published event {}: {}", event, ((Event) event).getLogMessage());
            }
        } else if (o instanceof Event) {
            storeEvent((Event) o);
            publishEvent((Event) o);
            log.info("Published event {}: {}", o, ((Event) o).getLogMessage());
        } else if (o instanceof RetreiveAggregateEvents) {
            readAggregateEvents((RetreiveAggregateEvents) o);

        } else if (o instanceof AcknowledgePreviousEventsProcessed) {
            sender().tell(new Success(), self());
        } else if (o instanceof UpgradeAggregate) {
            UpgradeAggregate upgrade = (UpgradeAggregate) o;
            log.info("Upgrading aggregate " + upgrade.getAggregateType());
            storage.upgradeFromOldStorage(upgrade.getAggregateType(), upgrade.getOldStorage());
            log.info("Upgraded aggregate " + upgrade.getAggregateType());
        } else if (o instanceof TakeBackup) {
            for (ActorRef actorRef : aggregateSubscribers.values()) {
                actorRef.tell(o, self());
            }
            storage.doBackup(((TakeBackup) o).getBackupdir(), "backupEventStore" + format.format(new Date()));

        } else if (o instanceof TakeSnapshot) {
            for (ActorRef actorRef : aggregateSubscribers.values()) {
                actorRef.tell(o, self());
            }
        } else if(o instanceof ClusterEvent.ClusterMetricsChanged){

        } else {
            log.debug("Unhandled message {}", o);
        }
    }


    private void tryToFillSubscription(final ActorRef sender, final Subscription subscription) {
        if (subscription instanceof LiveSubscription) {
            log.info("CompleteSubscriptionRegistered");
            sender.tell(new CompleteSubscriptionRegistered(subscription.getAggregateType()), self());
            addSubscriber(subscription);
        } else {
            log.info("Got subscription on {} from {}, filling subscriptions", subscription, sender);
            boolean finished = loadEvents(sender, subscription);
            if (!finished) {
                log.info("IncompleteSubscriptionPleaseSendNew");
                sender.tell(new IncompleteSubscriptionPleaseSendNew(subscription.getAggregateType()), self());
            } else {
                log.info("CompleteSubscriptionRegistered");
                sender.tell(new CompleteSubscriptionRegistered(subscription.getAggregateType()), self());
                addSubscriber(subscription);
            }
        }
    }

    private void readAggregateEvents(RetreiveAggregateEvents retreiveAggregateEvents) {
        final ActorRef sender = sender();

        sender.tell(storage.loadEventsForAggregateId(retreiveAggregateEvents.getAggregateType(), retreiveAggregateEvents.getAggregateId(), retreiveAggregateEvents.getFromJournalId()), self());
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

    private void publishEvents(StoreEvents events) {
        for (Event event : events.getEvents()) {
            publishEvent(event);
        }
    }

    private void publishEvent(Event event) {
        Set<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateType());
        if (actorRefs == null) {
            return;
        }
        sendEvent(event, actorRefs);
    }

    private void addSubscriber(Subscription subscription) {
        aggregateSubscribers.put(subscription.getAggregateType(), sender());
        log.info("Added subscriber {} " + subscription);
        log.info("Current subscribers " + aggregateSubscribers);
    }

    public void storeEvent(final Event event) {
        event.setCreated(new DateTime());
        storage.saveEvent(event);
    }

    private void storeEvents(StoreEvents o) {
        for (Event event : o.getEvents()) {
            event.setCreated(new DateTime());
        }
        storage.saveEvents(o.getEvents());
    }


    private void sendEvent(Event event, ActorRef subscriber) {
        Event upgadedEvent = upgradeEvent(event);
        log.debug("Publishing event {} to {}", upgadedEvent, subscriber);
        subscriber.tell(upgadedEvent, self());
    }

    private void sendEvent(Event event, Set<ActorRef> subscribers) {
        Event upgradedEvent = upgradeEvent(event);
        for (ActorRef subscriber : subscribers) {
            log.debug("Publishing event {} to {}", upgradedEvent, subscriber);
            subscriber.tell(upgradedEvent, self());
        }
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
}
