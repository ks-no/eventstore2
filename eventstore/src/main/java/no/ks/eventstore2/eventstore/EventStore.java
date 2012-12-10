package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.MemberStatus;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.json.Adapter;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EventStore extends UntypedActor {


	static final Logger log = LoggerFactory.getLogger(EventStore.class);
	private Gson gson = new Gson();
	private JdbcTemplate template;

	Map<String, List<ActorRef>> aggregateSubscribers = new HashMap<String, List<ActorRef>>();
	private boolean leader;
	private ActorRef leaderEventStore;

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
		updateLeaderState();
		subscribeToClusterEvents();
		log.debug("Eventstore started with adress {}", getSelf().path());
	}

	private void subscribeToClusterEvents() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			cluster.subscribe(self(), ClusterEvent.ClusterDomainEvent.class);
		} catch (ConfigurationException e) {

		}
	}

	private void updateLeaderState() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			leader = cluster.readView().isLeader();
			log.debug("Is leader? {}", leader);
			Address leaderAdress = cluster.readView().leader().get();
			log.debug("leader adress {}",leaderAdress);
			leaderEventStore = getContext().actorFor(leaderAdress.toString() + "/user/eventStore");
			log.debug("Leader eventstore is {}",leaderEventStore.path());
			log.debug("Member status is {}", cluster.readView().self().status());
			if(!leader && cluster.readView().self().status().equals(MemberStatus.up()))
				for (String s : aggregateSubscribers.keySet()) {
					leaderEventStore.tell(new SubscriptionRefresh(s,aggregateSubscribers.get(s)));
				}
		} catch (ConfigurationException e) {
			log.debug("Not cluster system");
			leader = true;
		}
	}

	public void onReceive(Object o) throws Exception {
		if( o instanceof ClusterEvent.LeaderChanged){
			updateLeaderState();
		} else if (o instanceof Event) {
			log.debug("Got event {}",o);
			if (leader) {
				storeEvent((Event) o);
				publishEvent((Event) o);
			} else {
				leaderEventStore.tell(o, sender());
			}
		} else if (o instanceof Subscription) {
			Subscription subscription = (Subscription) o;
			log.debug("Got subscription on {} from {}",  subscription.getAggregateId()  , sender().path());
			addSubscriber(subscription);
			if (leader)
				publishEvents(subscription.getAggregateId());
			else
				leaderEventStore.tell(subscription, sender());
		} else if (o instanceof SubscriptionRefresh) {
			SubscriptionRefresh subscriptionRefresh = (SubscriptionRefresh) o;
			log.debug("Refreshing subscription for {}", subscriptionRefresh);
			addSubscriber(subscriptionRefresh);
		} else if ("ping".equals(o)) {
			sender().tell("pong", self());
		}
	}

	private void addSubscriber(SubscriptionRefresh refresh) {
		if(!aggregateSubscribers.containsKey(refresh.getAggregateId())){
			aggregateSubscribers.put(refresh.getAggregateId(),new ArrayList<ActorRef>());
		}
		aggregateSubscribers.get(refresh.getAggregateId()).addAll(refresh.getSubscribers());
	}

	private void publishEvent(Event event) {
		List<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateId());
		if (actorRefs == null)
			return;
		for (ActorRef ref : actorRefs) {
			log.debug("Publishing event to {}", ref.path());
			ref.tell(event, self());
		}
	}

	private void addSubscriber(Subscription subscription) {
		if (!aggregateSubscribers.containsKey(subscription.getAggregateId()))
			aggregateSubscribers.put(subscription.getAggregateId(), new ArrayList<ActorRef>());

		aggregateSubscribers.get(subscription.getAggregateId()).add(sender());
	}

	private void publishEvents(String aggregateid) {
		for (Event event : loadEvents(aggregateid)) {
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