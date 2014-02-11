package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.UntypedActorFactory;
import no.ks.eventstore2.json.Adapter;
import no.ks.eventstore2.json.DateTimeTypeConverter;
import org.joda.time.DateTime;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class EventStoreFactory implements UntypedActorFactory {

	private DataSource ds;

	private List<Adapter> adapters = new ArrayList<Adapter>();

	public Actor create() {
       return new EventStore(new H2JournalStorage(ds, getAdapters()));
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

	public void addAdapter(Adapter adapter){
		adapters.add(adapter);
	}

    private List<Adapter> getAdapters() {
        List<Adapter> gsonAdapters = new ArrayList<Adapter>();
        Adapter jodaTimeAdapter = new Adapter(DateTime.class, new DateTimeTypeConverter());
        gsonAdapters.add(jodaTimeAdapter);
		gsonAdapters.addAll(adapters);
        return gsonAdapters;
    }
}
