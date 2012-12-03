package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.json.Adapter;
import org.joda.time.DateTime;
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

    private Gson gson = new Gson();
    private JdbcTemplate template;

    Map<String, List<ActorRef>> aggregateSubscribers = new HashMap<String, List<ActorRef>>();
	private List<ActorRef> remoteActors;

	public EventStore(DataSource dataSource, List<Adapter> adapters, List<ActorRef> remoteActors) {
		this.remoteActors = remoteActors;
		template = new JdbcTemplate(dataSource);
        GsonBuilder builder = new GsonBuilder();
        for(Adapter adapter : adapters){
            builder.registerTypeAdapter(adapter.getType(), adapter.getTypeAdapter());
        }
        gson = builder.create();
    }

    @Override
    public void preStart(){
		for (ActorRef remoteEventStore : remoteActors) {
			remoteEventStore.tell("Hi", self());
		}
        System.out.println(getSelf().path().toString());
    }

    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            storeEvent((Event) o);
            publishEvent((Event) o);
        } else if (o instanceof Subscription){
            Subscription subscription = (Subscription) o;

            publishEvents(subscription.getAggregateId());
            addSubscriber(subscription);
			System.out.println("Got subscription on " + subscription.getAggregateId() + " from " + sender().path());
			for (ActorRef remoteEventStore : remoteActors) {
				remoteEventStore.tell(new RemoteSubscription(subscription),self());
			}

        } else if(o instanceof RemoteSubscription){
			RemoteSubscription subscription = (RemoteSubscription) o;
			addSubscriber(new Subscription(subscription.getAggregateId()));
			System.out.println("Got remotesubscription on " + subscription.getAggregateId() + " from " + sender().path());

		} else if ("ping".equals(o)){
            sender().tell("pong", self());
        }  else if ("Hi".equals(o)){
			for (String aggregateid : aggregateSubscribers.keySet()) {
				sender().tell(new RemoteSubscription(aggregateid),self());
			}
		}
    }

    private void publishEvent(Event event) {
        List<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateId());
        if (actorRefs == null)
            return;
        for (ActorRef ref : actorRefs) {
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
            sender().tell(event,self());
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
