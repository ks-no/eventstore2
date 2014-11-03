package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import org.joda.time.DateTime;

public abstract class AbstractJournalStorage implements JournalStorage {

    protected Kryo kryo = new Kryo();

    public AbstractJournalStorage() {
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
    }

    public abstract void saveEvent(Event event);

    public abstract boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent);

    public abstract boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey);

    public abstract void open();

    public abstract void close();

    public abstract void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage);

    public abstract void doBackup(String backupDirectory, String backupfilename);

    public abstract EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId);
}
