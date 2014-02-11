package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.json.Adapter;
import no.ks.eventstore2.response.Success;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EventStore extends UntypedActor {


	static final Logger log = LoggerFactory.getLogger(EventStore.class);

	Map<String, HashSet<ActorRef>> aggregateSubscribers = new HashMap<String, HashSet<ActorRef>>();
	private ActorRef leaderEventStore;

    private AkkaClusterInfo leaderInfo;
    private JournalStorage storage;

    public static Props mkProps(JournalStorage journalStorage){
        return Props.create(EventStore.class, journalStorage);
    }

    @Deprecated
    public EventStore(DataSource dataSource, List<Adapter> adapters){
        storage = new H2JournalStorage(dataSource, adapters);
    }

    public EventStore(JournalStorage journalStorage) {
        storage = journalStorage;
	}

	@Override
	public void preStart() {
        leaderInfo = new AkkaClusterInfo(getContext().system());
        leaderInfo.subscribeToClusterEvents(self());
		updateLeaderState(null);
		log.debug("Eventstore started with adress {}", getSelf().path());
	}

	private void updateLeaderState(ClusterEvent.LeaderChanged leaderChanged) {
		try {
            leaderInfo.updateLeaderState(leaderChanged);
			leaderEventStore = getContext().actorFor(leaderInfo.getLeaderAdress() + "/user/eventstore");
			log.debug("LeaderEventStore is {}", leaderEventStore);

			if(!leaderInfo.isLeader() && leaderInfo.amIUp())
				for (String s : aggregateSubscribers.keySet()) {
					leaderEventStore.tell(new SubscriptionRefresh(s,aggregateSubscribers.get(s)),self());
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
		} else if (o instanceof Subscription) {
			Subscription subscription = (Subscription) o;
			addSubscriber(subscription);
			if (leaderInfo.isLeader()) {
                log.info("Got subscription on {} from {}, adding to pending subscriptions",  subscription.getAggregateId()  , sender().path());
                addPendingSubscription(sender(), subscription.getAggregateId());
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
			if(leaderEventStore != null) leaderEventStore.tell("ping",self());
		} else if(o instanceof AcknowledgePreviousEventsProcessed){
            if(leaderInfo.isLeader())
                sender().tell(new Success(),self());
            else
                leaderEventStore.tell(o,sender());
        }
	}

    private HashMap<String,HashSet<ActorRef>> pendingSubscriptions = new HashMap<String, HashSet<ActorRef>>();

    private void fillPendingSubscriptions() {
        if(pendingSubscriptions.isEmpty())return;
        log.info("Filling pending subscriptions {}", pendingSubscriptions);
        for (final String aggregateid : pendingSubscriptions.keySet()) {
            storage.loadEventsAndHandle(aggregateid, new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    sendEvent(event,pendingSubscriptions.get(aggregateid));
                }
            });
        }
        pendingSubscriptions.clear();
        log.info("Filled pending subscriptions");
    }

    private void addPendingSubscription(ActorRef subscriber, String aggregateId) {
        if(pendingSubscriptions.get(aggregateId) == null) pendingSubscriptions.put(aggregateId,new HashSet<ActorRef>());

        pendingSubscriptions.get(aggregateId).add(subscriber);

        getContext().system().scheduler().scheduleOnce(Duration.create(250, TimeUnit.MILLISECONDS),
                self(), "FillPendingSubscriptions", getContext().system().dispatcher(), self());
    }

    private void addSubscriber(SubscriptionRefresh refresh) {
		if(!aggregateSubscribers.containsKey(refresh.getAggregateId())){
			aggregateSubscribers.put(refresh.getAggregateId(),new HashSet<ActorRef>());
		}
		aggregateSubscribers.get(refresh.getAggregateId()).addAll(refresh.getSubscribers());
	}

	private void publishEvent(Event event) {
		Set<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateId());
		if (actorRefs == null)
			return;
		sendEvent(event,actorRefs);
	}

	private void addSubscriber(Subscription subscription) {
		if (!aggregateSubscribers.containsKey(subscription.getAggregateId()))
			aggregateSubscribers.put(subscription.getAggregateId(), new HashSet<ActorRef>());

		aggregateSubscribers.get(subscription.getAggregateId()).add(sender());
	}

    public void storeEvent(final Event event) {
		event.setCreated(new DateTime());
        storage.saveEvent(event);
	}

    private void sendEvent(Event event, Set<ActorRef> subscribers){
        event = upgradeEvent(event);
        for (ActorRef subscriber : subscribers) {
            log.debug("Publishing event {} to {}",event,subscriber);
            subscriber.tell(event, self());
        }
    }

    private Event upgradeEvent(Event event) {
        Event upgraded = event.upgrade();
        while(upgraded != event){
            event = upgraded;
            upgraded = event.upgrade();
        }
        return upgraded;
    }





}
