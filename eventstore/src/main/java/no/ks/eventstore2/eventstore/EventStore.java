package no.ks.eventstore2.eventstore;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
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
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

class EventStore extends UntypedActor {


	static final Logger log = LoggerFactory.getLogger(EventStore.class);
	private Gson gson = new Gson();
	private JdbcTemplate template;
    private Kryo kryov1 = new Kryo();
    private Kryo kryov2 = new Kryo();

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
        kryov1.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryov1.register(DateTime.class, new JodaDateTimeSerializer());
        kryov2.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryov2.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryov2.register(DateTime.class, new JodaDateTimeSerializer());
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
        if(!(o instanceof Subscription)){
            fillPendingSubscriptions();
        }
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
        for (String aggregateid : pendingSubscriptions.keySet()) {
            publishEvents(aggregateid, pendingSubscriptions.get(aggregateid));
        }
        pendingSubscriptions.clear();
        log.info("Filled pending subscriptions");
    }

    private void addPendingSubscription(ActorRef subscriber, String aggregateId) {
        if(pendingSubscriptions.get(aggregateId) == null) pendingSubscriptions.put(aggregateId,new HashSet<ActorRef>());

        pendingSubscriptions.get(aggregateId).add(subscriber);

        getContext().system().scheduler().scheduleOnce(Duration.create(250, TimeUnit.MILLISECONDS),
                self(), "FillPendingSubscriptions", getContext().system().dispatcher());
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

	private void publishEvents(String aggregateid, HashSet<ActorRef> actorRefs) {
		for (Event event : loadEvents(aggregateid)) {
            for (ActorRef subscriber : actorRefs) {
                log.debug("Publishing event {} from db to {}",event,subscriber);
                subscriber.tell(event, self());
            }
		}
	}

	public void storeEvent(final Event event) {
		event.setCreated(new DateTime());

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
		kryov2.writeClassAndObject(kryodata, event);
        kryodata.close();

		LobHandler lobHandler = new DefaultLobHandler();
		template.execute("INSERT INTO event (id,aggregateid, class, dataversion, kryoeventdata) VALUES(seq.nextval,?,?,?,?)",
				new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
					protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
						lobCreator.setBlobAsBytes(ps, 4, output.toByteArray());
						ps.setString(2, event.getClass().getName());
                        ps.setInt(3,2);
						ps.setString(1, event.getAggregateId());
					}
				}
		);

	}

	private List<Event> loadEvents(String aggregate) {
		return template.query("SELECT * FROM event WHERE aggregateid = ? ORDER BY ID", new Object[]{aggregate}, new RowMapper<Event>() {
			public Event mapRow(ResultSet resultSet, int i) throws SQLException {
                if(resultSet.getInt("dataversion") == 2){
                    Blob blob = resultSet.getBlob("kryoeventdata");

                    Input input = new Input(blob.getBinaryStream());
                    Event event = (Event) kryov2.readClassAndObject(input);
                    input.close();
                    return event;
                } else {
                    Blob blob = resultSet.getBlob("kryoeventdata");
                    Input input = new Input(blob.getBinaryStream());
                    Event event = (Event) kryov1.readClassAndObject(input);
                    input.close();
                    log.info("Read event {} as v1", event);
                    updateEventToKryo(resultSet.getInt("id"),event);
                    return event;
                }
			}
		});
	}

    private void updateEventToKryo(final int id, final Event event) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
        kryov2.writeClassAndObject(kryodata, event);
        kryodata.close();
        log.info("Updating {} to kryo data {}", id, event);
        LobHandler lobHandler = new DefaultLobHandler();
        template.execute("update event set dataversion=2, kryoeventdata=? where id=?",
                new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
                    protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
                        lobCreator.setBlobAsBytes(ps, 1, output.toByteArray());
                        ps.setInt(2, id);
                    }
                }
        );
    }

}
