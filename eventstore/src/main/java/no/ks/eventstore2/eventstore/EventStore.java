package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import no.ks.eventstore2.AkkaClusterInfo;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.json.Adapter;
import no.ks.eventstore2.response.Success;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class EventStore extends UntypedActor {


	static final Logger log = LoggerFactory.getLogger(EventStore.class);
	private Gson gson = new Gson();
	private JdbcTemplate template;

	Map<String, HashSet<ActorRef>> aggregateSubscribers = new HashMap<String, HashSet<ActorRef>>();
	private ActorRef leaderEventStore;

    private AkkaClusterInfo leaderInfo;

	public EventStore(DataSource dataSource, List<Adapter> adapters) {
		template = new JdbcTemplate(dataSource);
		GsonBuilder builder = new GsonBuilder();
		for (Adapter adapter : adapters) {
			builder.registerTypeAdapter(adapter.getType(), adapter.getTypeAdapter());
		}
		gson = builder.create();
	}

	@Override
	public void preStart() {
        leaderInfo = new AkkaClusterInfo(getContext().system());
        leaderInfo.subscribeToClusterEvents(self());
		updateLeaderState(null);
		log.debug("Eventstore started with adress {}", getSelf().path());
	}

	private void subscribeToClusterEvents() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			cluster.subscribe(self(), ClusterEvent.ClusterDomainEvent.class);
		} catch (ConfigurationException e) {

		}
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
		if( o instanceof ClusterEvent.LeaderChanged){
            log.info("Recieved LeaderChanged event: {}", o);
			updateLeaderState((ClusterEvent.LeaderChanged)o);
		} else if (o instanceof Event) {
			if (leaderInfo.isLeader()) {
				storeEvent((Event) o);
				publishEvent((Event) o);
                log.info("Published event {}", o);
			} else {
				log.info("Sending to leader {} event {}", sender(), o);
				leaderEventStore.tell(o, sender());
			}
		} else if (o instanceof Subscription) {
			Subscription subscription = (Subscription) o;
			addSubscriber(subscription);
			if (leaderInfo.isLeader()) {
                log.info("Got subscription on {} from {}",  subscription.getAggregateId()  , sender().path());
				publishEvents(subscription.getAggregateId());
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
		for (ActorRef ref : actorRefs) {
			log.debug("Publishing event to {}", ref.path());
			ref.tell(event, self());
		}
	}

	private void addSubscriber(Subscription subscription) {
		if (!aggregateSubscribers.containsKey(subscription.getAggregateId()))
			aggregateSubscribers.put(subscription.getAggregateId(), new HashSet<ActorRef>());

		aggregateSubscribers.get(subscription.getAggregateId()).add(sender());
	}

	private void publishEvents(String aggregateid) {
		for (Event event : loadEvents(aggregateid)) {
			log.debug("Publishing event {} from db to {}",event,sender());
			sender().tell(event, self());

		}
	}

	public void storeEvent(final Event event) {
		event.setCreated(new DateTime());
		final String json = gson.toJson(event);
		LobHandler lobHandler = new DefaultLobHandler();
		template.execute("insert into event (id,aggregateid,event, class) values(seq.nextval,?,?,?)",
				new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
					protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
						lobCreator.setClobAsString(ps, 2, json);
						ps.setString(3, event.getClass().getName());
						ps.setString(1, event.getAggregateId());
					}
				}
		);
	}

	private List<Event> loadEvents(String aggregate) {
		return template.query("select * from event where aggregateid = ? order by ID", new Object[]{aggregate}, new RowMapper<Event>() {
			public Event mapRow(ResultSet resultSet, int i) throws SQLException {
				String clazz = resultSet.getString("class");
				Class<?> classOfT;
				try {
					classOfT = Class.forName(clazz);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
				Clob json = resultSet.getClob("event");

				return (Event) gson.fromJson(json.getCharacterStream(), classOfT);
			}
		});
	}

}
