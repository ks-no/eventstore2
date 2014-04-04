package no.ks.eventstore2.eventstore;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import no.ks.eventstore2.AkkaClusterInfo;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.response.Success;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.Duration;
import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent;

import com.google.common.collect.HashMultimap;

public class EventStore extends UntypedActor {

	private static Logger log = LoggerFactory.getLogger(EventStore.class);

	private HashMultimap<String,ActorRef> aggregateSubscribers = HashMultimap.create();
	private ActorRef leaderEventStore;

    private AkkaClusterInfo leaderInfo;
    private JournalStorage storage;
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    public static Props mkProps(JournalStorage journalStorage){
        return Props.create(EventStore.class, journalStorage);
    }

    public EventStore(JournalStorage journalStorage) {
        storage = journalStorage;
	}

    @Override
    public void postStop() {
        storage.close();
    }

	@Override
	public void preStart() {
        leaderInfo = new AkkaClusterInfo(getContext().system());
        leaderInfo.subscribeToClusterEvents(self());
		updateLeaderState(null);
		log.debug("Eventstore started with adress {}", getSelf().path());
	}

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        log.warn("Restarted eventstore, restarting storage");
        storage.close();
        if(leaderInfo.isLeader()){
            // sleep so we are reasonably sure the other node has closed the storage
            try { Thread.sleep(500); } catch (InterruptedException e) {}
            storage.open();
        }
    }

    private void updateLeaderState(ClusterEvent.LeaderChanged leaderChanged) {
		try {
            leaderInfo.updateLeaderState(leaderChanged);
			leaderEventStore = getContext().actorFor(leaderInfo.getLeaderAdress() + "/user/eventstore");
			log.debug("LeaderEventStore is {}", leaderEventStore);

			if(!leaderInfo.isLeader() && leaderInfo.amIUp()){
				for (String s : aggregateSubscribers.keySet()) {
					leaderEventStore.tell(new SubscriptionRefresh(s,aggregateSubscribers.get(s)),self());
				}
            }

            if(leaderInfo.isLeader()){
                // sleep so we are reasonably sure the other node has closed the storage
                try { Thread.sleep(500); } catch (InterruptedException e) {}
                log.info("opening journal store");
                storage.open();
            }else {
                log.info("closing journal store");
                storage.close();
            }
		} catch (ConfigurationException e) {
			log.debug("Not cluster system");
		}
	}

	public void onReceive(Object o) throws Exception {
        if(!(o instanceof Subscription)){
            fillPendingSubscriptions();
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
        }
        if( o instanceof ClusterEvent.LeaderChanged){
            log.info("Recieved LeaderChanged event: {}", o);
			updateLeaderState((ClusterEvent.LeaderChanged)o);
		} else if (o instanceof Event) {
			if (leaderInfo.isLeader()) {
				storeEvent((Event) o);
				publishEvent((Event) o);
                log.info("Published event {}: {}", o, ((Event) o).getLogMessage());
			} else {
				log.info("Sending to leader {} event {}", sender(), o);
				leaderEventStore.tell(o, sender());
			}
		} else if(o instanceof RetreiveAggregateEvents){
            if (leaderInfo.isLeader()) {
                readAggregateEvents((RetreiveAggregateEvents)o);
            } else {
                log.info("Sending to leader {} retrieveAggregateEvents {}", sender(), o);
                leaderEventStore.tell(o, sender());
            }
        } else if (o instanceof Subscription) {
			Subscription subscription = (Subscription) o;
			addSubscriber(subscription);
			if (leaderInfo.isLeader()) {
				log.info("Got subscription on aggregate '" + subscription.getAggregateType() + "' from journalid '" + subscription.getFromJournalId() + "' with path '" + sender().path() + "'");
                tryToFillSubscription(sender(),subscription);
            } else {
                log.info("Sending subscription to leader {} from {}", leaderEventStore.path(), sender().path());
				leaderEventStore.tell(subscription, sender());
            }
		} else if (o instanceof SubscriptionRefresh) {
			SubscriptionRefresh subscriptionRefresh = (SubscriptionRefresh) o;
			log.info("Refreshing subscription for {}", subscriptionRefresh);
			addSubscriber(subscriptionRefresh);
		} else if ("ping".equals(o)) {
			log.debug("Ping reveiced from {}", sender());
			sender().tell("pong", self());
		} else if("pong".equals(o)){
			log.debug("Pong received from {}", sender());
		} else if("startping".equals(o)){
			log.debug("starting ping sending to {} from {}",leaderEventStore, self() );
			if(leaderEventStore != null) {
				leaderEventStore.tell("ping",self());
			}
		} else if(o instanceof AcknowledgePreviousEventsProcessed){
            if(leaderInfo.isLeader()) {
            	sender().tell(new Success(),self());
            } else {
            	leaderEventStore.tell(o,sender());
            }
        } else if(o instanceof UpgradeAggregate && leaderInfo.isLeader()){
            UpgradeAggregate upgrade = (UpgradeAggregate) o;
            log.info("Upgrading aggregate " + upgrade.getAggregateType());
            storage.upgradeFromOldStorage(upgrade.getAggregateType(), upgrade.getOldStorage());
            log.info("Upgraded aggregate " + upgrade.getAggregateType());
        } else if (o instanceof TakeBackup) {
            if (leaderInfo.isLeader()) {
                for (ActorRef actorRef : aggregateSubscribers.values()) {
                    actorRef.tell(o, self());
                }
                storage.doBackup(((TakeBackup) o).getBackupdir(), "backupEventStore"+ format.format(new Date()));
            } else {
                leaderEventStore.tell(o, sender());
            }
        } else if (o instanceof TakeSnapshot) {
            if (leaderInfo.isLeader()) {
                for (ActorRef actorRef : aggregateSubscribers.values()) {
                    actorRef.tell(o, self());
                }
            } else {
                leaderEventStore.tell(o, sender());
            }
        }
    }

    private void readAggregateEvents(RetreiveAggregateEvents retreiveAggregateEvents) {
        final ActorRef sender = sender();
        sender.tell(storage.loadEventsForAggregateId(retreiveAggregateEvents.getAggregateType(), retreiveAggregateEvents.getAggregateId(), retreiveAggregateEvents.getFromJournalId()),self());
    }

    private void tryToFillSubscription(final ActorRef sender, Subscription subscription) {
        boolean finished = false;
        if(subscription.getFromJournalId() == null || "".equals(subscription.getFromJournalId().trim())){
            finished = storage.loadEventsAndHandle(subscription.getAggregateType(), new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    sendEvent(event,sender);
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
        if(!finished){
            log.info("Subscription {} not Complete {} should ask for more",subscription,sender );
            sender.tell(new IncompleteSubscriptionPleaseSendNew(subscription.getAggregateType()),self());
            return;
        } else {
            sender.tell(new CompleteSubscriptionRegistered(subscription.getAggregateType()),self());
            addSubscriber(subscription);
        }
    }

    private Map<String,HashSet<ActorRef>> pendingSubscriptions = new HashMap<String, HashSet<ActorRef>>();

    private void fillPendingSubscriptions() {
        if(pendingSubscriptions.isEmpty()) {
        	return;
        }
        log.info("Filling pending subscriptions {}", pendingSubscriptions);
        for (final String aggregateType : pendingSubscriptions.keySet()) {
            storage.loadEventsAndHandle(aggregateType, new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    sendEvent(event,pendingSubscriptions.get(aggregateType));
                }
            });
        }
        pendingSubscriptions.clear();
        log.info("Filled pending subscriptions");
    }

    private void addPendingSubscription(ActorRef subscriber, String aggregateType) {
        if(pendingSubscriptions.get(aggregateType) == null) {
        	pendingSubscriptions.put(aggregateType,new HashSet<ActorRef>());
        }
        pendingSubscriptions.get(aggregateType).add(subscriber);

        getContext().system().scheduler().scheduleOnce(Duration.create(250, TimeUnit.MILLISECONDS),
                self(), "FillPendingSubscriptions", getContext().system().dispatcher(), self());
    }

    private void addSubscriber(SubscriptionRefresh refresh) {
        aggregateSubscribers.putAll(refresh.getAggregateType(), refresh.getSubscribers());
	}

	private void publishEvent(Event event) {
		Set<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateType());
		if (actorRefs == null) {
			return;
		}
		sendEvent(event,actorRefs);
	}

	private void addSubscriber(Subscription subscription) {
		aggregateSubscribers.put(subscription.getAggregateType(), sender());
	}

    public void storeEvent(final Event event) {
		event.setCreated(new DateTime());
        storage.saveEvent(event);
	}

    private void sendEvent(Event event, ActorRef subscriber) {
        Event upgadedEvent = upgradeEvent(event);
        log.debug("Publishing event {} to {}", upgadedEvent, subscriber);
        subscriber.tell(upgadedEvent, self());
    }

    private void sendEvent(Event event, Set<ActorRef> subscribers){
        Event upgradedEvent = upgradeEvent(event);
        for (ActorRef subscriber : subscribers) {
            log.debug("Publishing event {} to {}",upgradedEvent,subscriber);
            subscriber.tell(upgradedEvent, self());
        }
    }

    private Event upgradeEvent(Event event) {
        Event currentEvent = event;
        Event upgraded = currentEvent.upgrade();
        while(upgraded != currentEvent){
            currentEvent = upgraded;
            upgraded = currentEvent.upgrade();
        }
        return upgraded;
    }





}
