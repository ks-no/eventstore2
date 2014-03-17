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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class MongoDBJournal implements JournalStorage {
    private final Kryo kryo;
    private final DBCollection metaCollection;
    private DB db;

    private String dataversion = "01";

    private int eventReadLimit = 5000;
    private Logger log = LoggerFactory.getLogger(MongoDBJournal.class);

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates, int eventReadLimit) {
        this(db, registration, aggregates);
        this.eventReadLimit = eventReadLimit;
    }

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates) {
        this.db = db;
        db.setWriteConcern(WriteConcern.SAFE);
        for (String aggregate : aggregates) {
            db.getCollection(aggregate).ensureIndex("jid");
            db.getCollection(aggregate).ensureIndex("rid");
            db.getCollection(aggregate).setWriteConcern(WriteConcern.SAFE);
        }
        metaCollection = db.getCollection("journalMetadata");
        metaCollection.setWriteConcern(WriteConcern.SAFE);
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
        return outputs.toByteArray();
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value));
        return (Event) kryo.readClassAndObject(input);
    }

    @Override
    public void saveEvent(Event event) {
        DBCollection collection = db.getCollection(event.getAggregateId());
        event.setJournalid(String.valueOf(getNextJournalId(collection)));
        BasicDBObject doc = new BasicDBObject("jid", Long.parseLong(event.getJournalid())).
                append("rid", event.getAggregateRootId()).
                append("d", serielize(event));
        collection.insert(doc);
    }

    public void saveEvents(List<Event> events) {
        if(events == null || events.size() == 0) return;
        String agg = events.get(0).getAggregateId();
        DBCollection collection = db.getCollection(agg);
        long nextJournalId = getNextJournalId(collection);
        List<DBObject> dbObjectArrayList = new ArrayList<DBObject>();
        for (Event event : events) {
            event.setJournalid(String.valueOf(nextJournalId));
            BasicDBObject doc = new BasicDBObject("jid", nextJournalId).
                    append("rid", event.getAggregateRootId()).
                    append("d", serielize(event));
            nextJournalId++;
            dbObjectArrayList.add(doc);
        }
        collection.insert(dbObjectArrayList);
    }

    private long getNextJournalId(DBCollection collection) {
        return collection.getCount() + 1;
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateid, handleEvent, "0", eventReadLimit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateid, handleEvent, fromKey, eventReadLimit);
    }

    private boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent, String fromKey, int readlimit) {
        BasicDBObject query = new BasicDBObject("jid", new BasicDBObject("$gt", Long.parseLong(fromKey)));
        DBCursor dbObjects = db.getCollection(aggregateid).find(query).sort(new BasicDBObject("jid", 1)).limit(readlimit);
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
        return i < readlimit;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void upgradeFromOldStorage(String aggregateId, JournalStorage oldStorage) {
        DBCursor id = metaCollection.find(new BasicDBObject("_id", "upgrade_" + aggregateId));
        if (id.hasNext() && dataversion.equals(id.next().get("version"))) {
            log.info("Already upgraded aggregate " + aggregateId);
            return;
        }

        final ArrayList<Event> events = new ArrayList<Event>();
        boolean done = false;
        HandleEvent handleEvent = new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
                if(events.size() > eventReadLimit){
                    log.info("Events reached {}, saving away", eventReadLimit);
                    saveEvents(events);
                    events.clear();
                }
            }
        };
        done = oldStorage.loadEventsAndHandle(aggregateId, handleEvent);
        while (!done) {
            String lastJournalID = events.get(events.size()-1).getJournalid();
            log.info("saving to lastJournalId {}",lastJournalID);
            saveEvents(events);
            events.clear();
            done = oldStorage.loadEventsAndHandle(aggregateId, handleEvent,lastJournalID);

        }
        saveEvents(events);
        events.clear();
        metaCollection.save(new BasicDBObject("_id", "upgrade_" + aggregateId).append("version", dataversion));
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {

    }
}
