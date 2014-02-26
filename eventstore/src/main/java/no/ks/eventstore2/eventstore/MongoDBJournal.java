package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import com.mongodb.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

public class MongoDBJournal implements JournalStorage {
    private final Kryo kryo;
    private DB db;

    private int eventReadLimit = 5000;

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates, int eventReadLimit) {
        this(db,registration,aggregates);
        this.eventReadLimit = eventReadLimit;
    }

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates) {
        this.db = db;
        for (String aggregate : aggregates) {
            db.getCollection(aggregate).ensureIndex("jid");
        }
        kryo = new Kryo();
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        registration.registerClasses(kryo);
    }

    private byte[] serielize(Event event) {
        final ByteArrayOutputStream outputs = new ByteArrayOutputStream();
        ByteBufferOutput output = new ByteBufferOutput(outputs);
        kryo.writeClassAndObject(output, event);
        output.close();
        byte[] bytes = outputs.toByteArray();
        return bytes;
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value));
        return (Event) kryo.readClassAndObject(input);
    }

    @Override
    public void saveEvent(Event event) {
        DBCollection collection = db.getCollection(event.getAggregateId());
        event.setJournalid(String.valueOf(getNextJournalId(collection)));
        BasicDBObject doc = new BasicDBObject("a", event.getAggregateId()).
                append("c", event.getCreated()).
                append("jid", Long.parseLong(event.getJournalid())).
                append("d", serielize(event));
        collection.insert(doc);
    }

    private long getNextJournalId(DBCollection collection) {
        return collection.getCount() + 1;
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent) {
        DBCursor dbObjects = db.getCollection(aggregateid).find().sort(new BasicDBObject("jid",1));
        try {
            while (dbObjects.hasNext()) {
                DBObject next = dbObjects.next();
                handleEvent.handleEvent(deSerialize((byte[]) next.get("d")));
            }
        } finally {
            dbObjects.close();
        }
        return true;
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent, String fromKey) {
        BasicDBObject query = new BasicDBObject("jid", new BasicDBObject("$gt", Long.parseLong(fromKey)));
        DBCursor dbObjects = db.getCollection(aggregateid).find(query).sort(new BasicDBObject("jid",1)).limit(eventReadLimit);
        int i = 0;
        try {
            while (dbObjects.hasNext()) {
                DBObject next = dbObjects.next();
                handleEvent.handleEvent(deSerialize((byte[]) next.get("d")));
                i++;
            }
        } finally {
            dbObjects.close();
        }
        return i<eventReadLimit;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void upgradeFromOldStorage(String aggregateId, JournalStorage oldStorage) {
        oldStorage.loadEventsAndHandle(aggregateId,new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                saveEvent(event);
            }
        });
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {

    }
}
